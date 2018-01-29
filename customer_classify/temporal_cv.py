from datetime import datetime, timedelta
from s3_read_write import S3ReadWrite

def read_data(end_date, n_folds, offset,
    input_s3, input_csv_path, output_s3, output_csv_path):

    date_fmt = '%Y-%m-%d'
    end_date = datetime.strptime(end_date, date_fmt)
    dates = [(end_date - timedelta(days = offset * i)
            ).strftime(date_fmt)
        for i in range(n_folds)]

    input_data = [(input_s3.read_from_S3_csv(
            csv_path = input_csv_path,
            csv_name = date)
        .assign(input_date = date)
        .set_index(['internal_user_id', 'input_date']))
        for date in dates]

    output_data = [(output_s3.read_from_S3_csv(
            csv_path = output_csv_path,
            csv_name = date)
        .assign(input_date = date)
        .set_index(['internal_user_id', 'input_date']))
        for date in dates]

    return pd.concat(input_data), pd.concat(output_data)


def main():
    input_s3 = S3ReadWrite('plated-data-science', 'sample_input_data')
    output_s3 = S3ReadWrite('plated-data-science', 'sample_output_data')

    input_data, output_data = read_data(
        end_date = '2018-01-21', n_folds = 5, offset = 7,
        input_s3 = input_s3, input_csv_path = 'ETLV_v2',
        output_s3 = output_s3, output_csv_path = 'canceled_within_7_days')

if __name__ == '__main__':
    main()
