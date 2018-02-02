########################
### Extract Information
########################

import logging
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format = '{asctime} {name:12s} {levelname:8s} {message}',
    datefmt = '%m-%d %H:%M:%S',
    style = '{')

class PostgresConnector():
    def __init__(self):
        """Sets up connection information without initiating connection.
        Configures pandas to turn off settingwithoutcopy warning.
        """
        self.logger = logging.getLogger(__name__)
        self.driver = 'postgresql+psycopg2'
        self.db = 'production'
        self.host = 'localhost'
        self.port = 5439

    def _connect(self):
        connection = create_engine("{driver}://{host}:{port}/{dbname}".format(
            driver = driver, host = host, port = port, dbname = db))
        self.logger.info(self.db)
        return connection

    def _get_df(self, query, param_dict=None):
        """Returns a data frame with query results.
        Accepts query parameters as a dictionary.
        """
        self.logger.info("Connecting to database...")
        connection = self._connect()
        try:
            if param_dict:
                results_df = pd.read_sql_query(text(query), connection,
                    params = param_dict)
            else:
                results_df = pd.read_sql_query(text(query), connection)
        except psycopg2.Error as e:
            self.logger.error(e)
            connection.close()
        self.logger.info("Data retrieved successfully.")
        return results_df

def get_etlv_modified(self, params):
    """Returns a dataframe with all users' behavior, demographics and
    order history for active users who have received at least 1 box.
    """
    self.logger.info("Getting ETLV as of {}".format(
        params['end_date']))
    return self._get_df(extract_queries.ETLV_MODIFIED, params)
