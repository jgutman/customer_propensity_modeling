from sqlalchemy import create_engine, text
import logging, re, sys
from datetime import datetime, timedelta
from argparse import ArgumentParser
from upload_customer_lists import upload_eligible_subscribers
import pandas as pd


def split_and_aggregate(data):
    # features to take the sum of
    sum_features = []

    # features to take the average of
    average_features = []

    # features to take the max value
    max_features = []

    # features to take the min value
    min_features = []

    # features to determine if any are true
    any_features = []

    # features to determine if all are true
    all_features = []


def eligible_users_by_fold(query_file, input_dates, connection):
    query = text(open(query_file).read())
    eligible_users = [pd.read_sql(query, connection,
        params = {'start_date_input': dates[0],
                  'end_date_input': dates[1]},
        index_col = 'internal_user_id')
        for dates in input_dates]
    logging.info('eligible users found for {} folds'.format(
        len(eligible_users)))

    eligible_users = pd.concat(eligible_users,
        keys = [dates[1] for dates in input_dates],
        names = ['fold_end_date', 'internal_user_id'])

    all_eligible_users = (eligible_users.index
        .get_level_values('internal_user_id')
        .unique()
        .to_frame())
    logging.info('{} distinct eligible users found across all folds'.format(
        all_eligible_users.shape[0]))

    return eligible_users, all_eligible_users


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
    input_dates = [(i - timedelta(days = args.offset + args.input_width),
        i - timedelta(days = args.offset))
        for i in outcome_dates]
    start_date = input_dates[args.n_folds][0]
    end_date = input_dates[0][1]
    logging.info('start and end dates created for {} folds'.format(len(input_dates)))

    connection = create_engine("{driver}://{host}:{port}/{dbname}".format(
          driver = "postgresql+psycopg2",
          host = "localhost",
          port = 5439,
          dbname = "production"))
    logging.info('database connection initialized')

    users_by_fold, all_users = eligible_users_by_fold(
        args.eligibility, input_dates, connection)
    logging.info('eligible subscribers for model training and validation pulled')

    upload_eligible_subscribers(all_users, connection,
        str(start_date.date()), str(end_date.date()))
    logging.info('eligible subscribers uploaded to database')


if __name__ == '__main__':
    parser = ArgumentParser('Extract data for training and validating propensity model')
    parser.add_argument('--eligibility',
        help = 'path to sql query defining customer eligibility')
    parser.add_argument('--feature_extract',
        help = 'path to sql query extracting unaggregated input features')
    parser.add_argument('--outcome',
        help = 'path to sql query extracting all target outcomes')

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
    parser.add_argument('--input_width', type = int,
        help = 'interval width of input data in days',
        default = 56)
    parser.add_argument('--n_folds', type = int,
        help = 'number of folds for temporal CV',
        default = 10)

    args = parser.parse_args()
    main(args)
