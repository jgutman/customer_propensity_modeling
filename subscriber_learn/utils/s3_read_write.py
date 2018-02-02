from io import BytesIO, StringIO
from botocore.exceptions import ClientError
import logging
import pandas as pd
import boto3
import os

class S3ReadWrite:
    """
    S3ReadWrite is a wrapper for boto3 which includes methods
    for reading csv's in S3 directly
    param client: boto3 client to be configured via AWS CLI
    type client: boto3 client
    param resource: boto3 resource to be configured via AWS CLI
    type resource: boto3 resource
    param bucket: S3 bucket name
    type bucket: str
    """

    def __init__(self, bucket=os.getenv('S3_AIRFLOW_BUCKET')):
        self.client = boto3.client('s3')
        self.resource = boto3.resource('s3')
        self.bucket = bucket

    def __str__(self):
        msg = '(bucket: {})'.format(str(self.bucket))
        return msg

    def __eq__(self, other):
        if self.bucket == other.bucket:
            return True
        else:
            return False

    @property
    def client(self):
        return self._client

    @client.setter
    def client(self, new_client):
        self._client = new_client

    @property
    def resource(self):
        return self._resource

    @resource.setter
    def resource(self, new_resource):
        self._resource = new_resource

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, new_bucket):
        if new_bucket:
            self._bucket = new_bucket
        else:
            raise ValueError('Missing bucket name! Please specify')

    def read_from_S3_csv(
            self,
            csv_name,
            **read_csv_kwargs):
        value = self.client.get_object(
                Bucket=self.bucket,
                Key= csv_name)['Body'].read()
        logging.info('Reading from {0}'.format(csv_name))
        return pd.read_csv(BytesIO(value), **read_csv_kwargs)

    def put_dataframe_to_S3(
            self,
            csv_name,
            dataframe):
        csv_buffer = StringIO()
        logging.info('Writing to {0}'.format(csv_name))
        dataframe.to_csv(csv_buffer, index=False, header=True)
        self.resource.Bucket(
            self.bucket).put_object(
            Key=csv_name,
            Body=csv_buffer.getvalue())

    def append_to_csv(self, dataframe, csv_name):
        try:
            data = pd.concat([self.read_from_S3_csv(
                csv_name), dataframe], ignore_index=True)
            logging.info('{0} found. Appending data'.format(csv_name))
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                data = dataframe
                logging.info('{0} not found.'
                             'Creating new file'.format(csv_name))
            else:
                raise ex

        logging.info('Writing to {0}'.format(csv_name))
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)

        self.resource.Bucket(
            self.bucket).put_object(
            Key=csv_name,
            Body=csv_buffer.getvalue())
