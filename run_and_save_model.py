from customer_classify import model_data, pipeline_tools, reporting

import re, os, sys, logging
import pandas as pd
import numpy as np

from sklearn.pipeline import make_pipeline
from sklearn import ensemble, feature_selection, preprocessing
from sklearn.model_selection import RandomizedSearchCV
from argparse import ArgumentParser


def fit_pipeline(model_matrix, grid_path, pkldir,
    alg_id = 'debug', alg_name = 'screening_rf',
    scoring = 'roc_auc', # 'f1_micro',
    write_predictions = True, path = None, group = None):
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

    pipeline = make_pipeline(pipeline_tools.DummyEncoder(),
            preprocessing.Imputer(),
            feature_selection.VarianceThreshold(),
            ensemble.RandomForestClassifier(random_state = 1100))
    param_grid = pipeline_tools.build_param_grid(pipeline, grid_path)
    grid_search = GridSearchCV(pipeline, n_jobs = -1, cv = 5,
        param_grid = param_grid, scoring = scoring,
        # verbose output suppressed during multiprocessing
        verbose = 1) # show folds and model fits as they complete

    # Adjust test_size for debugging runs
    X_train, X_test, y_train, y_test, lb = model_data.split_data(
        model_matrix, test_size = .20)

    with pipeline_tools.Timer() as t:
        logging.info('fitting the grid search')
        grid_search.fit(X_train, y_train)

    logging.info(reporting.pickle_model(grid_search,
        pkldir, lb, alg_id, model_tag = alg_name))

    if write_predictions:
        engine = model_data.connect_to_database(path, group)
        train_results = reporting.get_results(grid_search, X_train, y_train, lb)
        test_results = reporting.get_results(grid_search, X_test, y_test, lb)
        logging.info(reporting.output_predictions(
            train_results, test_results, engine, alg_id = alg_id))
    return grid_search, lb


def main(args=None):
    parser = ArgumentParser()
    parser.add_argument('--dyaml', dest = 'data_yaml',
        nargs = '*', default = ['model_data_opts.yaml'],
        help = 'Path to the model data yaml file')
    parser.add_argument('--credpath', dest = 'path',
        default = eduanalytics.credentials_path,
        help = 'Path to the db credentials file')
    parser.add_argument('--credgroup', dest = 'group',
        default = eduanalytics.credentials_group,
        help = 'Name of group for db credentials file')
    parser.add_argument('--gridpath', dest = 'grid_path',
        default = 'grid_options.yaml',
        help = 'Path to the grid search options yaml file')
    parser.add_argument('--pkldir', dest = 'pkldir',
        default = eduanalytics.pkl_path,
        help = 'Path to store binary compressed model files')
    parser.add_argument('--fit', dest = 'train_model',
        default = False, action = 'store_true',
        help = 'Train the model from scratch')
    parser.add_argument('--predict', dest = 'predict_new',
        default = False, action = 'store_true',
        help = 'Generate predictions on new data')
    parser.add_argument('--id', dest = 'alg_id', type = int,
        nargs = '*', default = None,
        help = 'Algorithm id for pre-trained models')
    args = parser.parse_args()

    logging.basicConfig(format = "%(asctime)s\t %(message)s",
        level = logging.DEBUG, datefmt = "%m/%d/%y %I:%M:%S %p")

    #engine = model_data.connect_to_database(args.path, args.group)

    if args.train_model:
        alg_id_list = []
        pipelines = []
        for dyaml in args.data_yaml:
            model_matrix, alg_id, alg_name = model_data.get_data_for_modeling(
                filename = dyaml,
                # by default, sqlalchemy.create_engine has no default timeout
                engine = model_data.connect_to_database(args.path, args.group))
            alg_id_list.append(alg_id)
            pipelines.append(
                fit_pipeline(model_matrix, args.grid_path,
                args.pkldir, alg_id, alg_name,
                path = args.path, group = args.group))
    else:
        alg_id_list = args.alg_id
        pipelines = [reporting.load_model(args.pkldir, alg_id)
            for alg_id in alg_id_list]

    if args.predict_new:
        for pipeline, dyaml, alg_id in zip(
                pipelines, args.data_yaml, alg_id_list):
            logging.info(reporting.write_current_predictions(
                pipeline[0], filename = dyaml,
                conn = model_data.connect_to_database(args.path, args.group),
                label_encoder = pipeline[1], alg_id = alg_id))

if __name__ == '__main__':
    main()
