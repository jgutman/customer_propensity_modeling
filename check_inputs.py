def summarize_features(X):
    col_info = pd.DataFrame(
        'column' = X.columns,
        'dtype' = X.dtypes,
        'pct_missing' = X.isnull().mean(),
        'count_unique' = X.nunique(),
        'minimum' = X.min(),
        'maximum' = X.max()
    )
    return col_info
    
