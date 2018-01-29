import pandas as pd
import string, os, re, logging
import yaml, json, itertools
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelBinarizer


def convert_categorical(data):
    """Converts binary and string columns to Pandas Categoricals.
    Args:
        data (Pandas.DataFrame): a dataframe containing some binary
            (of any dtype) or string columns to be converted to categorical
    Returns:
        Pandas.DataFrame: dataframe with mix of categoricals and numeric cols
    """
    binary_cols = set(col for col in data if (
        data[col].nunique() == 2 and not re.search('_[ABCDF]$', col)))
    string_cols = set(col for col in data if data[col].dtype == 'O')
    categoricals = binary_cols.union(string_cols)
    data_categorical = data.astype({col: 'category' for col in categoricals})
    return data_categorical


def describe_model(filename, engine):
    """Reads in a new model specification file, parses it, and appends a basic
    description of the model (including a list of all its features) to the
    algorithms table of the database.
    Args:
        filename (str): a path to the model specification file in yaml format
        engine (sqlalchemy.Engine): a connection to the MySQL database
    Returns:
        dict: the model options specified in the file in dictionary format
        int: the algorithm id for the model just added
    """
    with open(filename) as f:
        model_opts = yaml.load(f)

    algorithm_name = model_opts['algorithm_name']
    algorithm_description = json.dumps(model_opts)

    feature_tbls = {"'vw$features${}'".format(tbl_name): cols_to_drop
        for tbl_name, cols_to_drop
        in model_opts['features'].items()}

    column_query = """select column_name
    from information_schema.columns
    where table_name in ({feature_string})
    and column_name not in ('aamc_id', 'application_year');""".format(
        feature_string = ", ".join(feature_tbls.keys()))

    column_names = pd.read_sql_query(column_query, engine)
    column_names = set(column_names.column_name)
    drop_cols = set(itertools.chain(*feature_tbls.values()))

    if drop_cols:
        column_names = column_names - drop_cols

    algorithm_details = json.dumps(list(column_names))

    x = pd.DataFrame({'algorithm_name': [algorithm_name],
            'algorithm_description': [algorithm_description],
            'algorithm_details': [algorithm_details]})
    x.to_sql('algorithm', engine, index = False, if_exists = 'append')

    algorithm_id = pd.read_sql_query('select max(id) from algorithm',
        engine).iloc[0,0]
    return model_opts, algorithm_id


def get_data_for_modeling(filename, engine):
    """Return a dataframe containing features specified by the yaml file for
    records meeting the cohort criteria specified in the yaml file.
    Includes the true outcome label from the database.
    Args:
        filename (str): path to YAML file with cohort, outcome, and
            feature specification for desired model data
        engine (sqlalchemy.Engine): a connection to the MySQL database
    Returns:
        Pandas.DataFrame: dataframe with Multi-index of aamc id and application year
            for applicants with known outcomes and qualifying cohort variables
        int: the algorithm id for the model specified by the file
        str: the algorithm name for the model specified by the file
    """
    model_opts, algorithm_id = describe_model(filename, engine)

    cohort_vals = model_opts['cohorts']['included']
    get_cohort = """select aamc_id, application_year
        from `vw$cohorts${cohort_tbl}`
        where {cohort_col} in ({cohort_vals})
        and fit_or_predict = 'fit'""".format(
            cohort_tbl = model_opts['cohorts']['tbl'],
            cohort_col = model_opts['cohorts']['col'],
            cohort_vals = ",".join(["'{}'".format(i) for i in cohort_vals]))

    get_outcomes = """select *
        from `vw$outcomes${outcome_tbl}`
        where (aamc_id, application_year) in
        ({cohort_query})""".format(
            outcome_tbl = model_opts['outcomes'],
            cohort_query = get_cohort)

    outcome_data = pd.read_sql_query(get_outcomes, engine,
        index_col = ['aamc_id', 'application_year'])

    features = loop_through_features(engine, model_opts['features'],
        subquery = get_cohort)

    model_data = outcome_data.join(features)
    model_data = convert_categorical(model_data)
    logging.info("pulled training/validation data for {n} applicants in {ncol} features".format(
        n = model_data.shape[0], ncol = model_data.shape[1] - 1))
    return model_data, algorithm_id, model_opts['algorithm_name']


def get_data_for_prediction(filename, engine, algorithm_id,
        prediction_tbl = "out$predictions$screening_current_cohort"):
    """Return a dataframe for the desired data for members of the current data
    for whom predictions have not already been generated containing the features
    specified in the model yaml file.
    Args:
        filename (str): path to YAML file with cohort, outcome, and
            feature specification for desired model data
        engine (sqlalchemy.Engine): a connection to the MySQL database
        algorithm_id (int): the algorithm id for the model specified in the file
            and used to generate predictions
        prediction_tbl (str): the name of the table where previous predictions
            have been written
    Returns:
        Pandas.DataFrame: dataframe with Multi-index (aamc id, application year)
            for applicants with known outcomes and qualifying cohort variables
    """
    with open(filename) as f:
        model_opts = yaml.load(f)

    cohort_vals = model_opts['cohorts']['included']
    get_cohort = """select aamc_id, application_year
        from `vw$cohorts${cohort_tbl}`
        where {cohort_col} in ({cohort_vals})
        and fit_or_predict = 'predict'""".format(
            cohort_tbl = model_opts['cohorts']['tbl'],
            cohort_col = model_opts['cohorts']['col'],
            cohort_vals = ",".join(["'{}'".format(i) for i in cohort_vals]))

    current_applicants_query = """select aamc_id, application_year
        from `vw$filtered${eligible_tbl}`
        where (aamc_id, application_year, {alg_id}) not in
        (select aamc_id, application_year, algorithm_id
        from `{prediction_tbl}`)
        and (aamc_id, application_year) in
        ({cohort_query})""".format(
            eligible_tbl = model_opts['predictions'],
            alg_id = algorithm_id,
            prediction_tbl = prediction_tbl,
            cohort_query = get_cohort)
    n_applicants = pd.read_sql_query(
        current_applicants_query, engine).shape[0]
    if n_applicants == 0:
        return pd.DataFrame()

    features = loop_through_features(engine, model_opts['features'],
        subquery = current_applicants_query)

    current_data = features[0].join(features[1:])
    logging.info(
        "pulled new testing data for {n} applicants in {ncol} features".format(
        n = current_data.shape[0], ncol = current_data.shape[1]))
    return current_data


def loop_through_features(engine, features_dict, subquery):
    """
    Args:
        engine (sqlalchemy.Engine): a connection to the mySQL database
        features_dict (dict(list[str])): a dictionary where the keys are the
            names of the feature tables and the values are lists of names of
            columns that should be excluded for each table (may be empty to
            include all features in the table)
        subquery (str): a string containing the subquery giving the aamc_id and
            application_year of the applicants of interest
    Returns:
        list(pandas.DataFrame): a list of dataframes containing all the features
            specified in the feature dictionary for all the applicants returned
            by the given subquery
    """
    feature_tbls = {"vw$features${}".format(tbl_name): cols_to_drop
        for tbl_name, cols_to_drop
        in features_dict.items()}

    features = list()
    for feature_tbl, drop_cols in feature_tbls.items():
        get_features = """select * from `{feature_tbl}`
        where (aamc_id, application_year) in ({query})""".format(
            feature_tbl = feature_tbl,
            query = subquery)
        feature_data = pd.read_sql_query(get_features, engine,
            index_col = ['aamc_id', 'application_year'])
        if drop_cols:
            feature_data.drop(drop_cols, axis = 1, inplace = True)
        features.append(feature_data)
    return features


def split_data(model_matrix, outcome_name = 'outcome',
        seed = 1100, test_size = .2):
    """Splits a data set into training and test and separates features (X)
    from target variable (y).
    Args:
        model_matrix (Pandas.DataFrame): data containing features and outcome
        outcome_name (str): name of column containing outcome variable
        seed (int): integer for random state variable
        test_size (float): proportion of data to hold out for testing
    Returns:
        Pandas.DataFrame: training features
        Pandas.DataFrame: testing features
        numpy.ndarray: training target labels (if multiclass, a column for each class)
        numpy.ndarray: testing target labels (if multiclass, a column for each class)
        sklearn.LabelBinarizer: transforms multiclass labels into binary dummies
    """
    X, y = model_matrix.drop(outcome_name, axis = 1), model_matrix[outcome_name]
    X_train, X_test, y_train, y_test = train_test_split(X, y,
            test_size = test_size, random_state = seed, stratify = y)
    lb = LabelBinarizer().fit(y_train)
    y_train, y_test = lb.transform(y_train).squeeze(), lb.transform(y_test).squeeze()
    return X_train, X_test, y_train, y_test, lb
