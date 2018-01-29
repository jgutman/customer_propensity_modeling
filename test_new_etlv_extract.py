import boto3
import pandas as pd
import sys
from pathlib import Path
from io import StringIO
from sqlalchemy import create_engine, text
from pandas.io.sql import get_schema
import logging
from argparse import ArgumentParser
from s3_read_write import S3ReadWrite
from datetime import datetime, timedelta

def main(args):
    logging.basicConfig(
        level=logging.INFO,
        format = '{asctime} {name:12s} {levelname:8s} {message}',
        datefmt = '%m-%d %H:%M:%S',
        style = '{',
        stream=sys.stdout)

    err = logging.StreamHandler(sys.stderr)
    err.setLevel(logging.ERROR)
    out = logging.StreamHandler(sys.stdout)
    out.setLevel(logging.INFO)
    logging.getLogger(__name__).addHandler(out)
    logging.getLogger(__name__).addHandler(err)

    outcome_dates = [args.outcome_date - timedelta(days = args.offset * i)
        for i in range(args.n_folds + 1)]
    outcome_dates = [d.strftime('%Y-%m-%d') for d in outcome_dates]

    connection = create_engine("{driver}://{host}:{port}/{dbname}".format(
          driver = "postgresql+psycopg2",
          host = "localhost",
          port = 5439,
          dbname = "production"))
    logging.info('database connection initialized')


    query = text(open(args.feature_extract).read())

    s3_writer = S3ReadWrite(bucket = 'plated-data-science',
        folder = 'sample_input_data')
    logging.info('S3ReadWrite created in {}'.format(str(s3_writer)))

    for date in outcome_dates:
        data = pd.read_sql_query(query, connection,
            params = {'end_date': date})
        logging.info('data pulled for {}'.format(date))
        s3_writer.put_dataframe_to_S3(csv_path = 'ETLV_v2',
            csv_name = date, dataframe = data)
        logging.info('data saved for {}'.format(date))


if __name__ == '__main__':
    parser = ArgumentParser('Extract data for training and validating propensity model')
    parser.add_argument('--feature_extract',
        help = 'path to sql query extracting input features')

    def valid_date(s):
        try:
            return datetime.strptime(s, '%Y-%m-%d')
        except ValueError:
            msg = 'Not a valid date: "{}".'.format(s)
            raise argparse.ArgumentTypeError(msg)

    parser.add_argument('--outcome_date', type = valid_date,
        help = 'latest outcome date to predict YYYY-MM-DD')
    parser.add_argument('--offset', type = int,
        help = 'delay between input data and outcome in days',
        default = 7)
    parser.add_argument('--n_folds', type = int,
        help = 'number of folds for temporal CV',
        default = 10)

    args = parser.parse_args()
    main(args)
