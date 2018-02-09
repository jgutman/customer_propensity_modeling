from sklearn.pipeline import TransformerMixin, Pipeline
from sklearn.base import BaseEstimator
import pandas as pd
import re, yaml, logging
from datetime import datetime, timedelta

class CVPipeline(Pipeline):
    def __init__(self, steps):
        super().__init__(steps)
        self.param_grid = dict()

    def set_param_grid(self, grid):
        param_grid = {
            '{step}__{option}'.format(
                step = step, option = option): value
            for step, step_options in grid.items()
            for option, value in step_options.items()
            if step in self.named_steps
        }
        self.param_grid = param_grid

    def refit(self, X):
        self.best_estimator_ = clone(self.estimator).set_params(
                **self.best_params_)
        final_fold = self.cv
        self.best_estimator_.fit(X, **fit_params)

    def extract_step(self, step_name):
        return self.named_steps.get(step_name)

    def describe(self):
        return self.named_steps.keys()

    def extract_model(self):
        """Extract the object corresponding to the final model estimator
        (classifier or regressor) for the best fitted estimator of the pipeline.
        """
        model_step_name = [step for step in self.describe()
            if step.endswith('classifier') or step.endswith('regressor')][-1]
        model = self.extract_step(model_step_name)
        return model

    def extract_encoder(self):
        """Extract the encoder step used to transform categorical variables into
        numerical dummy variables."""
        encoder_step_name = [step for step in self.describe()
            if step.endswith('encoder')][-1]
        encoder = self.extract_step(encoder_step_name)
        return encoder

    def extract_transformed_columns(self):
        """Returns a list of column names in the transformed data after applying an
        encoder transformation to the data."""
        return self.extract_encoder().transformed_columns


class DummyEncoder(BaseEstimator, TransformerMixin):
    """A one-hot encoder transformer with fit and transform methods.
    Suitable for use in a pipeline. Adds indicator variables for NAs,
    drops dummy for first level of categorical.
    Usage:
        d = DummyEncoder().fit(X_train)
        X_train_enc, X_test_enc = d.transform(X_train), d.transform(X_test)
    """
    def __init__(self, max_n = 15):
        self.columns = None
        self.transformed_columns = None
        self.other = dict()
        self.max_n = int(max_n)

    def collapse_categories(self, col):
        x = col.value_counts()
        if x.shape[0] > self.max_n:
            keep = x.iloc[:self.max_n].index
            # changes the dictionary in place
            self.other[col.name] = keep

    def otherize(self, col, X, replacement = "other"):
        # changes the data in place! make a copy before running
        X.loc[~X[col].isin(self.other[col]), col] = replacement

    def transform(self, X, y=None, **kwargs):
        logging.debug('Getting dummies: transform data')
        X = X.copy()
        [self.otherize(col, X) for col in self.other.keys()]

        transformed = pd.get_dummies(X, dummy_na = True)
        empty_cols = self.transformed_columns.difference(transformed.columns)
        if empty_cols.any():
            transformed[empty_cols] = 0

        transformed = transformed[self.transformed_columns]
        n_dummies = len(transformed.columns) - len(X.columns) + len(self.columns)
        logging.debug('Transformed {} dummies out of {} features'.format(
            n_dummies, len(self.columns)))
        return transformed

    def fit(self, X, y=None, **kwargs):
        self.columns = X.select_dtypes(
            include = ['object', 'category']).columns

        X[self.columns].apply(lambda x: self.collapse_categories(x))

        logging.debug('Getting dummies: fit encoder')
        X = X.copy()
        [self.otherize(col, X) for col in self.other.keys()]

        transformed = pd.get_dummies(X, dummy_na = True, sparse = True)
        # drop columns ending in _other
        self.transformed_columns = pd.Index([col for col in transformed.columns
            if not col.endswith('_other')])

        n_dummies = len(self.transformed_columns) - len(X.columns) + len(self.column
        logging.debug('Fit {} dummies out of {} features'.format(
            n_dummies, len(self.columns)))
        return self


class Timer(object):
    """A Timer object that begins timing when entered and ends timing recording
    elapsed time when exited.

    Usage:
        with Timer() as t:
            # do something
    """
    def __init__(self, name=None):
        self.name = name
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = datetime.now()
        return self

    def time_check(self):
        elapsed = self.end_time - self.start_time
        msg = 'Time elapsed: {hh} hours, {mm} minutes, {ss} seconds'.format(
            hh = elapsed.seconds//3600,
            mm = (elapsed.seconds//60) % 60,
            ss = elapsed.seconds % 60)
        return msg

    def __exit__(self, type, value, traceback):
        if self.name:
            logging.info("{}: ".format(self.name))
        self.end_time = datetime.now()
        logging.info(self.time_check())
