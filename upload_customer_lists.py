import boto3
import logging
from io import BytesIO, StringIO
from sqlalchemy import create_engine, text
from pandas.io.sql import get_schema
from pathlib import Path
from s3_read_write import S3ReadWrite


def upload_to_s3( data, start_date, end_date,
        bucket = 'plated-redshift-etl',
        dir = 'manual', subdir = 'propensity_model_subscribers'):
    s3_writer = S3ReadWrite(bucket = bucket, folder = dir)
    logging.info('S3ReadWrite created in {}'.format(str(s3_writer)))
    csv_name = 'eligible_users_{}_to_{}.csv'.format(start_date, end_date)
    s3_writer.put_dataframe_to_S3(csv_path = subdir,
        csv_name = csv_name, dataframe = data)
    fullpath = str(Path(dir, subdir, csv_name))
    logging.info('data saved in S3 bucket at {}'.format(fullpath))
    return bucket, fullpath


def upload_to_redshift( bucket, filename, tbl_name, engine, data,
        usernames, iam = 308127741254, role = 'RedshiftCopy'):
    iam_role = 'arn:aws:iam::{iam}:role/{role}'.format(
        iam = iam , role = role)

    tbl = tbl_name if tbl_name.startswith('analytics.') \
        else 'analytics.{}'.format(tbl_name)
    create_table_query = (get_schema(data, tbl, con = engine)
        .replace('"{}"'.format(tbl), tbl))

    copy_data_query = """ COPY {table_name}
    FROM 's3://{bucket}/{filename}'
         iam_role '{iam_role}'
         CSV BLANKSASNULL IGNOREHEADER AS 1 COMPUPDATE ON TIMEFORMAT 'auto'
         FILLRECORD STATUPDATE ON""".format(
         table_name = tbl,
         bucket = bucket,
         filename = filename,
         iam_role = iam_role)

    grant_privilege_queries = ["GRANT SELECT ON TABLE {table} to {user}".format(
        table = tbl, user = username )
        for username in usernames]

    with engine.begin() as connection:
        connection.execute(create_table_query)
        logging.info('Created empty table {}'.format(tbl))
        connection.execute(copy_data_query)
        logging.info("Data copied from s3://{bucket}/{filename} to {table}".format(
            bucket = bucket, filename = filename, table = tbl))
        [connection.execute(grant_select)
            for grant_select in grant_privilege_queries]
        logging.info('SELECT privileges granted to {}'.format(
            " ,".join(usernames)))


def replace_table( data, engine, bucket, filename, tbl_name, usernames ):
    drop_table = "DROP TABLE IF EXISTS analytics.{}".format(tbl_name)
    with engine.begin() as connection:
        connection.execute(drop_table)
    logging.info('Table analytics.{} dropped'.format(tbl_name))

    upload_to_redshift(bucket = bucket, filename = filename,
        tbl_name = tbl_name, engine = engine,
        data = data, usernames = usernames)
    logging.info('Table analytics.{} replaced'.format(tbl_name))


def upload_eligible_subscribers(data, engine, start_date, end_date,
        usernames = ['production_read_only', 'analytics_team']):
    # upload to S3
    bucket, filename = upload_to_s3( data, start_date, end_date )
    # replace table
    replace_table( data, engine, bucket, filename,
        tbl_name = 'propensity_model_subscribers',
        usernames = ['production_read_only', 'analytics_team'])
