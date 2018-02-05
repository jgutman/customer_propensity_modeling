from datetime import datetime, timedelta
from subscriber_learn.utils.s3_read_write import S3ReadWrite
import pandas as pd
import numpy as np
from sklearn.pipeline import make_pipeline
from sklearn import linear_model, feature_selection, preprocessing
from subscriber_learn.cross_validation import pipeline_tools

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
        self.groups = check_array(groups, ensure_2d=False, dtype=None)

        for fold_index in range(self.n_splits):
            train_indices = range(fold_index, fold_index + n_weeks)
            test_index = fold_index + n_weeks
            yield np.where(np.isin(groups, train_indices))[0], \
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

    check_cols = ['valid_account_creation',
        'valid_prospect_creation',
        'valid_accepted_terms']

    input_data = pd.concat(input_data)

    input_data = (input_data.loc[
        input_data.valid_account_creation & \
        input_data.valid_prospect_creation & \
        input_data.valid_accepted_terms, :]
        .drop(check_cols, axis = 1))

    response_data = (pd.concat(response_data)
        .loc[input_data.index])

    return input_data, response_data


def fit_pipeline(bucket = None, **op_kwargs, **context):
    input_data, response_data = read_data(
        s3_reader = S3ReadWrite(bucket),
        **op_kwargs,
        **context)

    pipeline = make_pipeline(pipeline_tools.DummyEncoder(),
            preprocessing.Imputer(strategy = 'median'),
            preprocessing.RobustScaler(),
            feature_selection.VarianceThreshold(threshold = .04),
            linear_model.ElasticNetCV(
                #l1_ratio = [.1, .5, .7, .9, .95, 1],
                cv = get_temporal_cv(input_data),
                n_jobs = -1,
                verbose = 1,
                random_state = 1100))

    with pipeline_tools.Timer() as t:
        pipeline.fit(input_data, response_data)


def main():
    bucket = 'plated-data-science'
    op_kwargs = {'n_folds': 5,
        'offset': 7,
        'input_dir': 'input_data/ETLV_v2',
        'response_dir':'response_data/canceled_within_7_days'}
    context = {'ds': '2018-02-02',
               'execution_date': datetime(2018, 1, 24, 0, 0)}

    fit_pipeline(bucket = bucket, **op_kwargs, **context)


if __name__ == '__main__':
    main()
