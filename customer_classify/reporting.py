import pandas as pd
import numpy as np
from eduanalytics import model_data, pipeline_tools
import os, fnmatch
from sklearn.externals import joblib

def get_results(clf, X, y, lb):
    """Takes a trained model, model matrix, and output data and
    returns a DataFrame of true and predicted output for all X, y
    Args:
        clf (sklearn.estimator): a fitted estimator with predict_proba
            method (could eventually be extended to decision_function)
        X (Pandas.DataFrame): df with index or Multi-index and all
            features needed for clf.predict_proba method
        y (numpy.ndarray): a numpy array transformed with lb.transform
            to hold a column for each class in lb.classes_, giving
            true outcome in binarized one-hot encoded format
        lb (sklearn.LabelBinarizer): a label binarizer holding the
            original class labels and transformation mechanism
    Returns:
        Pandas.DataFrame: a dataframe with index or Multi-index, and a
        column for the true outcome, as well as one column per class (in
        multiclass problems) giving the prediction score for that class
    """
    raw_scores = clf.predict_proba(X)
    if len(lb.classes_) > 2:
        scores = convert_multiclass_predictions(raw_scores, lb, X.index)
    else:
        scores = convert_binary_predictions(raw_scores, lb, X.index)
    if y is not None:
        y_flat = lb.inverse_transform(y)
        y_flat = pd.DataFrame(y_flat,
            columns = ['outcome'], index = X.index)
        scores = y_flat.join(scores)
    return scores


def convert_multiclass_predictions(scores, lb, ids):
    """Converts raw numpy array of one-hot encoded prediction scores to a
    one row per record Pandas DataFrame with index and column names
    Args:
        scores (list[Numpy array]): raw prediction scores in a list of
            numpy arrays, with each element of the list giving binary
            predictions in two columns i.e. (not class X, class X)
        lb (sklearn.LabelBinarizer): a label binarizer holding the
            original class labels
        ids (Pandas.index): an index or Multi-index for the raw scores
    Returns:
        Pandas.DataFrame: df with index and columns labeled
        predicted_{class_name} for each class in LabelBinarizer
    """
    scores_per_class = [p[:,1] for p in scores]
    scores_flat = np.vstack(scores_per_class).T
    class_names = ['predicted_{}'.format(p) for p in lb.classes_]
    scores_df = pd.DataFrame(scores_flat,
        columns = class_names, index = ids)
    return scores_df


def convert_binary_predictions(scores, lb, ids):
    """Converts raw numpy array of one-hot encoded prediction scores to a
    one row per record Pandas DataFrame with index and column names
    Args:
        scores (Numpy array): raw prediction scores with the second
            column giving the probability of the positive class
        lb (sklearn.LabelBinarizer): a label binarizer holding the
            original positive and negative class labels
        ids (Pandas.index): an index or Multi-index for the raw scores
    Returns:
        Pandas.DataFrame: df with index and 1 column labeled
        predicted_{class_name} for positive class in LabelBinarizer
    """
    class_names = 'predicted_{}'.format(lb.classes_[1])
    scores_df = pd.DataFrame(scores[:,1],
        columns = class_names, index = ids)
    return scores_df


def output_predictions(train_results, test_results, conn,
    alg_id = -1, tbl_name = 'screening_train_val'):
    """Write the true labels and prediction scores to a table in the database.
    Args:
        train_results (Pandas.DataFrame): indexed true and predicted output for
            all training data
        test_results (Pandas.DataFrame): indexed true and predicted output for
            all test data
        conn (sqlalchemy.Engine): a connection to the MySQL database
        alg_id (int): an algorithm id to store the model results by
        tbl_name (str): a name for the database table holding all results on
            the training and validation data
    Returns:
        str: the name of the table in the database
    """
    name = "out$predictions${}".format(tbl_name)
    train_results['set'] = 'train'
    test_results['set'] = 'test'
    results = pd.concat([train_results, test_results])
    results['algorithm_id'] = alg_id
    results.to_sql(name, conn, if_exists = 'append',
        index_label = results.index.names)
    return "Added to database {}: algorithm_id = {}".format(name, alg_id)


def write_current_predictions(clf, filename, conn, label_encoder, alg_id,
        tbl_name = 'screening_current_cohort'):
    """Write out the predictions for the new testing data, only if (aamc_id,
    application_year) does not already have a prediction score for that
    algorithm_id, including the overall score (pr(invite) - pr(reject))
    Args:
        clf (sklearn.GridSearchCV/Estimator): the unpickled model estimator
            that should be used to generate the predictions
        filename (str): path to model specification file
        conn (sqlalchemy.Engine): connection to the MySQL database
        label_encoder (sklearn.LabelBinarizer): the label binarizer object
            used to get the outcome names that correspond to predicted outcomes
        alg_id (int): the algorithm id for the model that should be used to
            generate the predictions
        tbl_name (str): name of table in database where predictions for all
            current applicants are written to
    Returns:
        str: output message confirming predictions have been written correctly
    """
    current_data = model_data.get_data_for_prediction(filename, conn, alg_id)
    if current_data.empty:
        return "No new applicant data for algorithm_id = {}".format(alg_id)
    results = get_results(clf, current_data, y = None, lb = label_encoder)

    name = "out$predictions${}".format(tbl_name)
    results = results.assign(algorithm_id = alg_id,
        score = lambda x: np.round(x.predicted_invite - x.predicted_reject, 2))
    results.to_sql(name, conn, if_exists = 'append',
        index_label = results.index.names)
    return "Added to database {}: algorithm_id = {}".format(name, alg_id)


def pickle_model(clf, pkl_path, label_encoder, alg_id, model_tag):
    """Write a sklearn object to disk in binary compressed format.
    Args:
        clf (sklearn.GridSearchCV/Estimator): the model to persist to disk
        pkl_path (str): name of the directory to store the pkl files
        tbl_name (str): shortname of the algorithm id for the model
        model_tag (str): algorithm name to tag the model with
    Returns:
        str: a message giving the path where the model has been saved
    """
    filename = "id{}_{}.pkl.z".format(alg_id, model_tag)
    model_plus_encoder = {'pipeline': clf, 'encoder': label_encoder}
    joblib.dump(model_plus_encoder,
        os.path.join(pkl_path, filename))
    output = "Written compressed model to: {} in {}".format(
        filename, pkl_path)
    return output


def load_model(pkl_path, alg_id):
    """Load a sklearn object from disk saved in binary compressed format.
    Args:
        pkl_path (str): name of the directory to store the pkl files
        alg_id (str): shortname of the algorithm_id for the model
    Returns:
        sklearn.GridSearchCV or Estimator: uncompressed sklearn model
    """
    pattern = "id{}_*.pkl.z".format(alg_id)
    filename = fnmatch.filter(os.listdir(pkl_path), pattern)
    model_plus_encoder = joblib.load(os.path.join(pkl_path, filename[0]))
    clf = model_plus_encoder['pipeline']
    encoder = model_plus_encoder['encoder']
    return clf, encoder
