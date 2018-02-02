from datetime import datetime, timedelta
from subscriber_learn.utils.s3_read_write import S3ReadWrite
import pandas as pd
import numpy as np
from sklearn.pipeline import make_pipeline
from sklearn import ensemble, feature_selection, preprocessing
from sklearn.model_selection import RandomizedSearchCV


class TemporalCrossValidator():
    def __init__(self, time_index):
        self.unique_groups, self.groups = np.unique(
            time_index, return_inverse=True)
        n_groups = len(self.unique_groups)
        self.n_splits = n_groups - 1
        if self.n_splits <= 1:
            raise ValueError(
                "Temporal cross-validation requires at least 3 "
                "temporal folds in the input data to form at "
                "least 2 train/test splits, found only {} time "
                "periods, {} splits".format(
                n_groups, self.n_splits))

    def split(self):
        self.groups = check_array(groups, ensure_2d=False, dtype=None)

        for train_index in range(self.n_splits):
            test_index = train_index + 1
            yield np.where(groups == train_index)[0], \
                np.where(groups == test_index)[0]


def get_temporal_cv(X, fold_col = 'input_date'):
    time_index = X.index.get_level_values(fold_col)
    cv = TemporalCrossValidator(time_index)
    return cv.split()


def read_data(s3_reader, n_folds, offset,
     input_dir, response_dir, **context):

    date_fmt = '%Y-%m-%d'
    dates = [context['execution_date'] - timedelta(days = offset * i)
        for i in range(n_folds)]

    input_data = [(s3_reader.read_from_S3_csv(
            csv_name = '{dir}/{year}/{month}/{day}/{date}.csv'.format(
            dir = input_dir,
            year = date.year,
            month = date.month,
            day = date.day,
            date = date.strftime(date_fmt)))
        .assign(input_date = date)
        .set_index(['internal_user_id', 'input_date']))
        for date in dates]

    response_data = [(s3_reader.read_from_S3_csv(
            csv_name = '{dir}/{year}/{month}/{day}/{date}.csv'.format(
            dir = response_dir,
            year = date.year,
            month = date.month,
            day = date.day,
            date = date.strftime(date_fmt)))
        .assign(input_date = date)
        .set_index(['internal_user_id', 'input_date']))
        for date in dates]

    input_data = pd.concat(input_data)
    response_data = pd.concat(response_data).loc[input_data.index]
    return input_data, response_data


def fit_pipeline(bucket = None, **op_kwargs, **context):
    """Train a new model over a grid search and optionally write train and test
    set predictions to the database.
    Args:
        model_matrix (Pandas.DataFrame): features and outcome variables for
            train and test set
        grid_path (str): path to file containing all pipeline options for the
            grid search as a yaml file
        pkldir (str): path to the directory to output the compressed model
            pickle file for the fitted model
        alg_id (str/int): an algorithm id to denote the fitted model in both
            the database and the saved pickle file
        alg_name (str): a short descriptor of the algorithm pulled from the
            model options file
        scoring (str): can also be a scorer callable object / function with
            signature scorer(estimator, X, y) for grid search optimization
        write_predictions (bool):
        path (str): credentials path to reconnect to the database in order to
            output predictions on train and test set
        group (str): credentials group to reconnect to the database
    Returns:
        (GridSearchCV, LabelBinarizer)
    """
    input_data, response_data = read_data(
        s3_reader = S3ReadWrite(bucket),
        **op_kwargs,
        **context)

    pipeline = make_pipeline(pipeline_tools.DummyEncoder(),
            preprocessing.Imputer(),
            feature_selection.VarianceThreshold(),
            linear_model.ElasticNet(random_state = 1100))

    param_grid = pipeline_tools.build_param_grid(pipeline, grid_path)
    grid_search = RandomizedSearchCV(pipeline,
        n_jobs = -1,
        cv = get_temporal_cv(input_data),
        param_grid = param_grid,
        scoring = scoring,
        # verbose output suppressed during multiprocessing
        verbose = 1) # show folds and model fits as they complete

    with pipeline_tools.Timer() as t:
        logging.info('fitting the grid search')
        grid_search.fit(input_data, response_data)

    reporting.pickle_model(grid_search,
        pkldir, lb, alg_id, model_tag = alg_name)


def main():
    bucket = 'plated-data-science'
    op_kwargs = {'n_folds': 5,
        'offset': 7,
        'input_dir': 'input_data/ETLV_v2',
        'response_dir':'response_data/canceled_within_7_days'}
    context = {'ds': '2018-01-24',
               'execution_date': datetime(2018, 1, 24, 0, 0)}

    fit_pipeline(bucket = bucket, **op_kwargs, **context)


if __name__ == '__main__':
    main()
