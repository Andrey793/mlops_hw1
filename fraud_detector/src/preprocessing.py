# Import standard libraries
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)
RANDOM_STATE = 42

def add_time_features(df):
    logger.debug('Adding time features...')
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    dt = df['transaction_time'].dt
    df['hour'] = dt.hour
    df['year'] = dt.year
    df['month'] = dt.month
    df['day_of_month'] = dt.day
    df['day_of_week'] = dt.dayofweek
    df.drop(columns='transaction_time', inplace=True)
    return df


def time_features(df):
    df['hour'] = pd.to_datetime(df['transaction_time']).dt.hour
    df['year'] = pd.to_datetime(df['transaction_time']).dt.year
    df['month'] = pd.to_datetime(df['transaction_time']).dt.month
    df['day_of_month'] = pd.to_datetime(df['transaction_time']).dt.day  
    df['day_of_week'] = pd.to_datetime(df['transaction_time']).dt.dayofweek
    return df


cat_cols = ['gender', 'merch', 'cat_id', 'one_city', 'us_state', 'jobs', 'street', 'name']
def run_preproc(df):
    df['name'] = df['name_1'] + ' ' + df['name_2']
    for col in cat_cols:
        df[col] = df[col].astype('category')
    df = time_features(df)
    df = df.drop(columns=['transaction_time', 'name_1', 'name_2'])
    logger.info('Preprocessing completed. Output shape: %s', df.shape)
    return df
