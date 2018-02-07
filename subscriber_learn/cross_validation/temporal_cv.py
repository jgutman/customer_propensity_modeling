import datetime
from ..utils.s3_read_write import S3ReadWrite
import pandas as pd
import numpy as np
from sklearn.pipeline import make_pipeline
from sklearn import linear_model, feature_selection, preprocessing
from .pipeline_tools import DummyEncoder, Timer
import logging

class TemporalCrossValidator():
    def __init__(self, time_index, n_weeks = 1):
        self.n_weeks = n_weeks
        self.unique_groups, self.groups = np.unique(
            time_index, return_inverse=True)
        n_groups = len(self.unique_groups)
        self.n_splits = n_groups - n_weeks
        if self.n_splits <= 1:
            raise ValueError(
                "Found {} time periods in the data, for training folds "
                "consisting of {} time periods of input per fold. "
                "Cross-validation requires 2 or more folds".format(
                n_groups, n_weeks))

    def split(self):
        for fold_index in range(self.n_splits):
            train_indices = range(fold_index, fold_index + self.n_weeks)
            test_index = fold_index + self.n_weeks
            yield np.where(np.isin(self.groups, train_indices))[0], \
                np.where(self.groups == test_index)[0]


def get_temporal_cv(X, n_weeks, fold_col = 'input_date'):
    time_index = X.index.get_level_values(fold_col)
    cv = TemporalCrossValidator(time_index, n_weeks)
    return cv.split()


def get_input_dates(end_date, offset, n_folds, n_weeks):
    return [end_date - datetime.timedelta(days = offset * i)
        for i in range(n_folds + n_weeks)]


def get_input_paths(dates, input_dir, filename = 'part000.csv000'):
    return {date: '{dir}/{year}/{month}/{day}/{filename}'.format(
        dir = input_dir,
        year = input_date.year,
        month = input_date.month,
        day = input_date.day,
        filename = filename)
        for date in dates}


def read_data(input_paths, response_paths, fold_col = 'input_date'):

    input_data = [(s3_reader.read_from_S3_csv(input_file)
        .assign(input_date = input_date)
        .set_index(['internal_user_id', fold_col]))
        for input_date, input_file in input_paths.items()]
    logging.info('{} input files read'.format(len(input_data)))

    response_data = [(s3_reader.read_from_S3_csv(response_file)
        .assign(input_date = date)
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


def fit_pipeline( n_folds, offset, n_weeks, input_dir, response_dir, **context ):
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{')

    dates = get_input_dates(context['execution_date'], offset, n_folds, n_weeks)
    input_paths = get_input_paths(dates, input_dir)
    response_paths = get_input_paths(dates, response_dir)
    input_data, response_data = read_data(input_paths, response_paths)

    estimator = linear_model.ElasticNetCV(
            l1_ratio = [.1, .5, .7, .9, .95, 1],
            cv = get_temporal_cv(input_data, n_weeks),
            n_jobs = -1,
            verbose = 1,
            random_state = 1100)

    pipeline = make_pipeline(DummyEncoder(),
            preprocessing.Imputer(strategy = 'median'),
            preprocessing.RobustScaler(),
            feature_selection.VarianceThreshold(threshold = .04),
            estimator,
            memory = '{dir}/pipeline {timestamp}'.format(
                dir = 'pickled_transformers',
                timestamp = datetime.datetime.now().strftime(
                '%y-%m-%d %H.%M.%S')))

    logging.info('Pipeline constructed with {} steps'.format(
        len(pipeline.named_steps)))

    with Timer('fit pipeline') as t:
        pipeline.fit(input_data, response_data)


def main():
    op_kwargs = {'n_folds': 5,
        'offset': 7,
        'n_weeks': 4,
        'input_dir': 'input_files/etlv_modified',
        'response_dir':'input_files/responses/canceled_within_7_days'}

    train_end_date = datetime.date(2018, 1, 21)
    context = {'ds': train_end_date.strftime('%Y-%m-%d'),
               'execution_date': train_end_date}

    fit_pipeline(
        n_folds = op_kwargs['n_folds'],
        offset = op_kwargs['offset'],
        n_weeks = op_kwargs['n_weeks'],
        input_dir = op_kwargs['input_dir'],
        response_dir = op_kwargs['response_dir'], **context)


if __name__ == '__main__':
    main()
