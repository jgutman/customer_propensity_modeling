from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np

from sklearn.model_selection import RandomizedSearchCV
from sklearn import linear_model, ensemble, feature_selection, preprocessing

from ..utils.s3_read_write import S3ReadWrite
from .pipeline_tools import *
from ..utils.model_persistence import *

class TemporalCrossValidator():
    def __init__(self, X, n_weeks = 1, fold_col = 'input_date'):
        time_index = X.index.get_level_values(fold_col)

        self.n_weeks = n_weeks
        self.unique_groups, self.groups = np.unique(
            time_index, return_inverse=True)

        logging.info('Cross-validating on:\n{}'.format(
            pd.Series(self.unique_groups)))
        n_groups = len(self.unique_groups)

        self.n_splits = n_groups - self.n_weeks
        if self.n_splits <= 1:
            raise ValueError(
                "Found {} time periods in the data, for training folds "
                "consisting of {} time periods of input per fold. "
                "Cross-validation requires 2 or more folds".format(
                n_groups, self.n_weeks))

    def split(self):
        for fold_index in range(self.n_splits):
            train_indices = range(fold_index, fold_index + self.n_weeks)
            test_index = fold_index + self.n_weeks
            logging.info('Fold {}: Train: {} Test: {}'.format(
                fold_index, list(train_indices), test_index))
            yield np.where(np.isin(self.groups, train_indices))[0], \
                np.where(self.groups == test_index)[0]

    def final_fold(self):
        fold = np.where(self.groups >= self.n_splits)[0]
        return fold


def get_input_dates(end_date, offset, n_folds, n_weeks):
    return [end_date - timedelta(days = offset * i)
        for i in range(n_folds + n_weeks)]


def get_input_paths(dates, input_dir, filename = 'part000.csv000'):
    return {date: '{dir}/{year}/{month}/{day}/{filename}'.format(
        dir = input_dir,
        year = date.year,
        month = date.month,
        day = date.day,
        filename = filename)
        for date in dates}


def read_data(input_paths, response_paths, fold_col = 'input_date'):
    s3_reader = S3ReadWrite()
    input_data = [(s3_reader.read_from_S3_csv(input_file)
        .assign(input_date = input_date)
        .set_index(['internal_user_id', fold_col]))
        for input_date, input_file in input_paths.items()]
    logging.info('{} input files read'.format(len(input_data)))

    response_data = [(s3_reader.read_from_S3_csv(response_file)
        .assign(input_date = input_date)
        .set_index(['internal_user_id', fold_col]))
        for input_date, response_file in response_paths.items()]
    logging.info('{} response files read'.format(len(response_data)))

    check_cols = ['valid_account_creation',
        'valid_prospect_creation',
        'valid_accepted_terms']

    input_data = pd.concat(input_data)

    input_data = (input_data.loc[
        input_data.valid_account_creation & \
        input_data.valid_prospect_creation & \
        input_data.valid_accepted_terms, :]
        .drop(check_cols, axis = 1))
    logging.info('Input shape: {}'.format(input_data.shape))

    response_data = (pd.concat(response_data)
        .loc[input_data.index]
        .squeeze())
    logging.info('Response shape: {}'.format(response_data.shape))

    return input_data, response_data


def fit_pipeline( n_folds, offset, n_weeks, input_dir, response_dir,
    model_name, grid_name, n_iter, n_jobs = -2, **context ):
    # n_jobs = -2 uses all but 1 cpu
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{')

    dates = get_input_dates(context['execution_date'], offset, n_folds, n_weeks)
    input_paths = get_input_paths(dates, input_dir)
    response_paths = get_input_paths(dates, response_dir)
    input_data, response_data = read_data(input_paths, response_paths)

    pipeline = CVPipeline([
        ('encoder', DummyEncoder()),
        ('imputer', preprocessing.Imputer()),
        ('scaler', preprocessing.RobustScaler()),
        ('feature_selector', feature_selection.VarianceThreshold()),
        ('random_forest', ensemble.RandomForestClassifier(random_state = 1100))
        ])

    logging.info('Pipeline constructed with {} steps'.format(
        len(pipeline.named_steps)))

    grid = ParamGridLoader().load_grid('models/grids', grid_name)
    pipeline.set_param_grid(grid)
    logging.info('Parameters: {}'.format(pipeline.param_grid))

    cv = TemporalCrossValidator(input_data, n_weeks)
    final_fold = cv.final_fold()

    grid_search = RandomizedSearchCV(pipeline,
        n_jobs = n_jobs, n_iter = n_iter, refit = False,
        cv = cv.split(), scoring = 'roc_auc',
        param_distributions = pipeline.param_grid,
        # verbose output suppressed during multiprocessing
        verbose = 3) # show folds and model fits as they complete

    persisted_at = datetime.now()
    pkl_path = 'models/pkls/{model_name}/{year}/{month}/{day}'.format(
        model_name = model_name,
        year = persisted_at.year,
        month = persisted_at.month,
        day = persisted_at.day)

    logging.info('Beginning partial grid search')
    logging.info('Sampling {} param settings'.format(n_iter))
    with Timer('Fit CVPipeline') as t:
        grid_search.fit(input_data, response_data)
        logging.info('Cross-validation completed. Best CV AUC: {0:.3f}'.format(
            grid_search.best_score_))
        logging.info('Best params: {}'.format(grid_search.best_params_))

    # Final model using cv best params and final temporal period
    # sets grid_search.best_estimator_ using grid_search.best_params_
    print(type(grid_search))
    grid_search.refit(final_fold, X = input_data, y = response_data)
    S3Pickler().dump(grid_search.best_estimator_, pkl_path, model_name)


def main():
    op_kwargs = {'n_folds': 5,
        'offset': 7,
        'n_weeks': 4,
        'n_iter': 2,
        'model_name': 'DEBUG_canceled_within_7_days_v1',
        'grid_name': 'simple_random_forest',
        'input_dir': 'input_files/etlv_modified',
        'response_dir': 'input_files/responses/canceled_within_7_days'}

    train_end_date = datetime(2018, 1, 31)
    context = {'ds': train_end_date.strftime('%Y-%m-%d'),
               'execution_date': train_end_date}

    fit_pipeline(
        n_folds = op_kwargs['n_folds'],
        offset = op_kwargs['offset'],
        n_weeks = op_kwargs['n_weeks'],
        n_iter = op_kwargs['n_iter'],
        model_name = op_kwargs['model_name'],
        grid_name = op_kwargs['grid_name'],
        input_dir = op_kwargs['input_dir'],
        response_dir = op_kwargs['response_dir'], **context)


if __name__ == '__main__':
    main()
