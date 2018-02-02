from sklearn.pipeline import TransformerMixin
from sklearn.base import BaseEstimator
import pandas as pd
import re, yaml, logging
import time, datetime

def extract_step_from_pipeline(cv_pipeline, step_name):
    """Extract the object corresponding to an explicitly named step from the
    modeling pipeline.
    Args:
        cv_pipeline (sklearn.GridSearchCV): a GridSearchCV object with an embedded
            Pipeline object containing a list of named steps
        step_name (str): a step name contained in the list of steps for the best
            fitted estimator found under GridSearch
    Returns:
        obj: an object with a .fit() and .transform() method with parameters
            selected by grid search
    """
    steps = cv_pipeline.best_estimator_.named_steps
    return steps.get(step_name)


def describe_pipeline_steps(cv_pipeline):
    """Returns a list of names of all steps in a fitted modeling pipeline.
    Args:
        cv_pipeline (sklearn.GridSearchCV): a GridSearchCV object with an embedded
            Pipeline object containing a list of named steps
    Returns:
        list[str]: a list of strings containing all pipeline step names
    """
    steps = cv_pipeline.best_estimator_.named_steps
    return list(steps.keys())


def extract_model_from_pipeline(cv_pipeline):
    """Extract the object corresponding to the final model estimator
    (classifier or regressor) for the best fitted estimator of the pipeline.
    Args:
        cv_pipeline (sklearn.GridSearchCV): a GridSearchCV object with an embedded
            Pipeline object containing a list of named steps
    Returns:
        sklearn.estimator: a fitted model estimator object with a predict()
            method (and for classifiers, predict_proba() or decision_function())
    """
    steps = describe_pipeline_steps(cv_pipeline)
    pattern = "(classifier|regressor)$"
    prog = re.compile(pattern)
    model_step_name = [step for step in steps if bool(prog.search(step))]
    model_step = extract_step_from_pipeline(cv_pipeline, model_step_name[-1])
    return model_step


def extract_encoder_from_pipeline(cv_pipeline):
    """Extract the encoder step used to transform categorical variables into
    numerical dummy variables.
    Args:
        cv_pipeline (sklearn.GridSearchCV): a GridSearchCV object with an embedded
            Pipeline object containing a list of named steps
    Returns:
        DummyEncoder: an encoder object with a fit(), transform(), and
            inverse_transform() method to convert between categorical variables
            and their dummy indicators
    """
    steps = describe_pipeline_steps(cv_pipeline)
    pattern = "encoder$"
    prog = re.compile(pattern)
    encoder_step_name = [step for step in steps if bool(prog.search(step))]
    encoder_step = extract_step_from_pipeline(
        cv_pipeline, encoder_step_name[-1])
    return encoder_step


def get_transformed_columns(cv_pipeline):
    """Returns a list of column names in the transformed data after applying an
    encoder transformation to the data.
    Args:
        cv_pipeline (sklearn.GridSearchCV): a GridSearchCV object with an embedded
            Pipeline object containing a list of named steps
    Returns:
        pandas.Index: an index of all column names in the transformed data
    """
    encoder_step = extract_encoder_from_pipeline(cv_pipeline)
    return encoder_step.transformed_columns


def build_param_grid(pipeline, grid_path):
    """Looks up pipeline steps in grid options yaml file and builds the
    appropriate parameter grid for steps in the pipeline.
    Args:
        pipeline (sklearn.Pipeline): a pipeline object with named_steps attribute
        grid_path: path to a yaml file containing the grid options to use
    Returns:
        dict: dictionary with step names as keys and parameter options as values
    """
    with open(grid_path, 'r') as f:
        grid = yaml.load(f)
    steps = set(pipeline.named_steps.keys())

    param_grid = dict()
    for step in steps.intersection(grid.keys()):
        options = grid.get(step)
        new_options = {'{step_name}__{option_name}'.format(
            step_name = step,
            option_name = option): value for option, value in
            options.items()}
        param_grid.update(new_options)
    return param_grid


class DummyEncoder(BaseEstimator, TransformerMixin):
    """A one-hot encoder transformer with fit and transform methods.
    Suitable for use in a pipeline. Adds indicator variables for NAs,
    drops dummy for first level of categorical.
    Usage:
        d = DummyEncoder().fit(X_train)
        X_train_enc, X_test_enc = d.transform(X_train), d.transform(X_test)
    """
    def __init__(self):
        self.columns = None
        self.transformed_columns = None

    def transform(self, X, y=None, **kwargs):
        transformed = pd.get_dummies(X,
            columns = self.columns,
            drop_first = False, # do not drop in transform method!
            dummy_na = True)
        transformed = transformed.loc[:,self.transformed_columns]
        return transformed

    def fit(self, X, y=None, **kwargs):
        self.columns = X.select_dtypes(
            include = ['object', 'category']).columns

        transformed = pd.get_dummies(X,
            columns = self.columns,
            drop_first = False, # need to be careful about dropping this
            dummy_na = True)
        self.transformed_columns = transformed.columns
        return self


class Timer(object):
    """A Timer object that begins timing when entered and ends timing adding
    elapsed time to a log when exited.

    Usage:
        with Timer() as t:
            # do something
    """
    def __init__(self, name=None):
        self.name = name
        self.start_time = None
        self.end_time = None

    def __enter__(self):
        self.start_time = time.time()
        return self

    def time_check(self):
        return time.time() - self.start_time

    def __exit__(self, type, value, traceback):
        if self.name:
            logging.info("{}: ".format(self.name))
        self.end_time = self.time_check()
        logging.info('Time elapsed: {}'.format(
            datetime.timedelta(seconds=int(self.end_time))))
