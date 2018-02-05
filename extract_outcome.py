from subscriber_learn.utils.s3_read_write import S3ReadWrite
import pandas as pd
from datetime import datetime, timedelta

date_fmt = '%Y-%m-%d'
s3_reader = S3ReadWrite('plated-data-science')
input_dir = 'input_data/ETLV_v2'
response_dir = 'response_data/canceled_within_7_days'
x0 = datetime(2018, 2, 2, 0, 0)

n_folds = 15
offset = 7
window = 7
outcome_dates = [x0 - timedelta(days = offset * i)
                for i in range(n_folds)]

input_dates = [outcome - timedelta(days = window)
            for outcome in outcome_dates]

outcomes = zip(input_dates, outcome_dates)

all_dates = set(input_dates).union(set(outcome_dates))

data = {date: s3_reader.read_from_S3_csv(
        csv_name = '{dir}/{year}/{month}/{day}/{date}.csv'.format(
            dir = input_dir,
            year = date.year,
            month = date.month,
            day = date.day,
            date = date.strftime(date_fmt)),
        usecols = ['internal_user_id'])
    for date in all_dates}

def build_output(input_date, output_date):
    input_users = data[input_date]
    output_users = data[output_date]
    input_users['canceled'] = ~input_users.isin(output_users)
    return input_users

outcome_data = {input_date: build_output(
        input_date, outcome_date)
    for input_date, outcome_date in outcomes}

[s3_reader.put_dataframe_to_S3(
    csv_name = '{dir}/{year}/{month}/{day}/{date}.csv'.format(
        dir = response_dir,
        year = date.year,
        month = date.month,
        day = date.day,
        date = date.strftime(date_fmt)),
    dataframe = outcome)
    for date, outcome in outcome_data.items()]
