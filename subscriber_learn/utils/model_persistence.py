from .s3_read_write import S3ReadWrite
from io import BytesIO
from sklearn.externals import joblib
from ruamel.yaml import YAML
import numpy as np
import scipy.stats as sp
import os, logging


class S3Pickler(S3ReadWrite):
    def __init__(self, bucket = 'plated-data-science'):
        super().__init__(bucket)

    def load(self, path, filename):
        filename = '{}/{}.pkl.z'.format(path, filename)
        value = self.client.get_object(
            Bucket = self.bucket,
            Key= filename)['Body'].read()
        logging.info('Found file at {} in {}'.format(filename, str(self)))
        return joblib.load(BytesIO(value))

    def dump(self, obj, path, filename):
        f = BytesIO()
        joblib.dump(obj, f, compress = True)
        filename = '{}/{}.pkl.z'.format(path, filename)

        self.resource.Bucket(
            self.bucket).put_object(
            Key = filename,
            Body = f.getvalue())
        logging.info('Dumped file to {} in {}'.format(filename, str(self)))


class ParamGridLoader(S3ReadWrite):
    def __init__(self, bucket = 'plated-data-science'):
        super().__init__(bucket)

    def load_grid(self, path, filename):
        filename = '{}/{}.yaml'.format(path, filename)
        value = self.client.get_object(
            Bucket = self.bucket,
            Key= filename)['Body'].read()

        raw_grid = YAML().load(BytesIO(value))
        processed_grid = {
            step: {
                option: eval(value)
                if type(value) == str else value
                for option, value in nested.items()
            }
            for step, nested in raw_grid.items()
        }
        logging.info('Found file at {} in {}'.format(filename, str(self)))
        return processed_grid

    def load_grid_local(self, local_path, s3_path, filename):
        with open(local_path, 'rb') as f:
            raw_grid = YAML().load(f)

        self.dump_grid(raw_grid, s3_path, filename)
        return dict(raw_grid)

    def dump_grid(self, grid, path, filename):
        f = BytesIO()
        YAML().dump(grid, f)
        filename = '{}/{}.yaml'.format(path, filename)

        self.resource.Bucket(
            self.bucket).put_object(
            Key = filename,
            Body = f.getvalue())
        logging.info('Dumped file to {} in {}'.format(filename, str(self)))
