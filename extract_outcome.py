from s3_read_write import S3ReadWrite
import pandas as pd
from datetime import datetime, timedelta

date_fmt = '%Y-%m-%d'
s3_input = S3ReadWrite('plated-data-science', 'sample_input_data')
s3_output = S3ReadWrite('plated-data-science', 'sample_output_data')
input_folder = 'ETLV_v2'
x0 = datetime.strptime('2018-01-28', date_fmt)

n_folds = 12
offset = 7
outcome_dates = [x0 - timedelta(days = offset * i)
                for i in range(n_folds)]

input_dates = [outcome - timedelta(days = offset)
            for outcome in outcome_dates]

folds = [(a.strftime(date_fmt), b.strftime(date_fmt))
            for a,b in zip(input_dates, outcome_dates)]

all_dates = set(x[0] for x in folds).union(set(x[1] for x in folds))

data = {date: s3_input.read_from_S3_csv(
    csv_path = 'ETLV_v2',
    csv_name = date,
    usecols = ['internal_user_id'])
    for date in all_dates}

def build_output(input_date, output_date):
    input = data[input_date]
    output = data[output_date]
    input['canceled'] = ~input.internal_user_id.isin(
        output.internal_user_id)
    return input

output = {date1: build_output(date1, date2)
    for date1, date2 in folds}

[s3_output.put_dataframe_to_S3(
    'canceled_within_{}_days'.format(offset),
    date, outcome_data)
    for date, outcome_data in output.items()]
