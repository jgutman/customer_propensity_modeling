import eduanalytics
from eduanalytics import model_data, pipeline_tools, reporting
from lime import lime_tabular
import pandas as pd
from sklearn import preprocessing
from collections import namedtuple

## Read in model, model data, outcomes, and predictions

def load_model_and_results(tbl_name,
            cred_path = eduanalytics.credentials_path,
            cred_group = eduanalytics.credentials_group,
    pkl_path = '/Volumes/IIME/EDS/data/admissions/pkls'):

    engine = model_data.connect_to_database(cred_path, cred_group)
    results = model_data.get_data_for_modeling(engine,
            tbl_name, status = 'predictions')
    data = model_data.get_data_for_modeling(engine, tbl_name)
    grid_search = reporting.load_model(pkl_path, tbl_name, 'rf_grid')
    return grid_search, data, results


def get_components_from_model(grid_search):
    forest = pipeline_tools.extract_model_from_pipeline(grid_search)
    encoder = pipeline_tools.extract_step_from_pipeline(grid_search, 'dummyencoder')
    imputer = pipeline_tools.extract_step_from_pipeline(grid_search, 'imputer')
    return encoder, imputer, forest


def split_data_and_results(data, results):
    train_results = results[results.set == 'train']
    test_results = results[results.set == 'test']
    outcome = data.outcome.astype(float)
    data = data.drop('outcome', axis = 1)

    X_train = data.loc[train_results.index]
    X_test = data.loc[test_results.index]
    y_train = outcome.loc[train_results.index].astype(float)
    y_test = outcome.loc[test_results.index].astype(float)
    Dataset = namedtuple('Dataset', ['X', 'y', 'output'])

    train = Dataset(X_train, y_train, train_results)
    test = Dataset(X_test, y_test, test_results)
    return train, test


### Extract categories and prepare dicts for numeric and categorical cols
def get_categorical_and_numeric_dicts(data, encoder):
    categorical_cols = set(encoder.columns)
    transformed_columns = encoder.transformed_columns
    all_numeric_cols = set(data.columns) - categorical_cols
    numeric_cols = all_numeric_cols.intersection(set(transformed_columns))

    categorical_dict = {index: list(data[col].cat.categories)
                        for index, col in enumerate(data.columns)
                        if col in categorical_cols}
    categorical_index = list(categorical_dict.keys())

    numeric_dict = {index: transformed_columns.get_loc(col)
                    for index, col in enumerate(data.columns)
                    if col in numeric_cols}
    numeric_index = list(numeric_dict.keys())

    ColInfo = namedtuple('ColInfo', ['colnames', 'mapping', 'index'])
    categorical = ColInfo(categorical_cols, categorical_dict, categorical_index)
    numeric = ColInfo(numeric_cols, numeric_dict, numeric_index)

    return categorical, numeric


def add_missing_category(data, encoder, categorical_dict):
    categorical_including_nan = categorical_dict.copy()
    transformed_columns = encoder.transformed_columns
    cols_with_missing = [col.split('_nan')[0] for col in transformed_columns
                         if col.endswith('_nan')]
    for col in cols_with_missing:
        index = data.columns.get_loc(col)
        categorical_including_nan[index].append('nan')
    return categorical_including_nan


### Transform, impute, and encode data from raw to Lime-ready format
def impute_numeric(data, numeric_dict, imputer, encoder):
    # data and imputed_data are Pandas DataFrames
    # transformed_and_imputed is a numpy ndarray (2d)
    imputed_data = data.copy()
    transformed_and_imputed = imputer.transform(
        encoder.transform(data))

    for raw_index, imputed_index in numeric_dict.items():
        imputed_data.iloc[:,raw_index] = transformed_and_imputed[:,imputed_index]
    return imputed_data


def encode_data(data, categorical_names):
    encoded_data = data.copy()
    le = preprocessing.LabelEncoder()
    for index, values in categorical_names.items():
        le.fit([str(v) for v in values])
        col = data.iloc[:,index].astype(str)
        encoded_data.iloc[:,index] = le.transform(col)
    return encoded_data.fillna(0)


def transform_data(data, categorical_cols, colnames):
    transformed_data = pd.DataFrame(data, columns = colnames)
    dtypes = {col: 'category' if col in categorical_cols else 'float'
              for col in colnames}
    transformed_data = transformed_data.astype(dtypes)
    encoder_new = pipeline_tools.DummyEncoder()
    encoder_new.fit(transformed_data)
    transformed_data = encoder_new.transform(transformed_data)
    return transformed_data.as_matrix()


### Running Lime
def build_explainer(train, test, imputer, encoder, class_labels):
    categorical, numeric = get_categorical_and_numeric_dicts(train, encoder)
    new_mapping = add_missing_category(train, encoder, categorical.mapping)
    categorical = categorical._replace(mapping = new_mapping)

    encoded_train = impute_encode(train, categorical, numeric,
                                    imputer, encoder)
    encoded_test = impute_encode(test, categorical, numeric,
                                    imputer, encoder)
    encoded_data = encoded_train.append(encoded_test)

    explainer = lime_tabular.LimeTabularExplainer(
        encoded_train.as_matrix(),
        feature_names = list(encoded_train.columns),
        class_names = class_labels,
        categorical_features = categorical.index,
        categorical_names = categorical.mapping,
        kernel_width=3)

    return encoded_data, explainer, categorical


def impute_encode(dataset, categorical, numeric, imputer, encoder):
    imputed = impute_numeric(dataset, numeric.mapping, imputer, encoder)
    encoded = encode_data(imputed, categorical.mapping)
    return encoded


def explain_instance(id, dataset, explainer,
                     categorical, forest, n_features = 5):
    colnames = list(dataset.columns)
    predict_fn = lambda X: forest.predict_proba(transform_data(X, categorical.colnames, colnames))
    row = dataset.loc[id,:]
    exp = explainer.explain_instance(row, predict_fn, num_features=n_features)
    exp.show_in_notebook(show_all=False)
