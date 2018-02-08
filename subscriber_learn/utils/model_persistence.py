from .s3_read_write import S3ReadWrite
from io import BytesIO
from sklearn.externals import joblib
from ruamel.yaml import YAML
import numpy as np
import scipy.stats as sp
import os


class S3Pickler(S3ReadWrite):
    def __init__(self, bucket = 'plated-data-science')):
        super().__init__(bucket)

    def load(self, path, filename):
        value = self.client.get_object(
            Bucket = self.bucket,
            Key= '{}/{}.pkl.z'.format(path, filename)
            )['Body'].read()
        return joblib.load(BytesIO(value))

    def dump(self, obj, path, filename):
        f = BytesIO()
        joblib.dump(obj, f, compress = True)

        self.resource.Bucket(
            self.bucket).put_object(
            Key = '{}/{}.pkl.z'.format(path, filename),
            Body = f.getvalue())


class ParamGridLoader(S3ReadWrite):
    def __init__(self, bucket = 'plated-data-science'):
        super().__init__(bucket)

    def load_grid(self, path, filename):
        value = self.client.get_object(
            Bucket = self.bucket,
            Key= '{}/{}.yaml'.format(path, filename)
            )['Body'].read()

        raw_grid = YAML().load(BytesIO(value))
        processed_grid = {step: {option: eval(value)
            if type(value) == str else value
            for option, value in nested.items()}
            for step, nested in raw_grid.items()}
        return processed_grid

    def dump_grid(self, grid, path, filename):
        f = BytesIO()
        YAML().dump(grid, f)

        self.resource.Bucket(
            self.bucket).put_object(
            Key = '{}/{}.yaml'.format(path, filename),
            Body = f.getvalue())
