import pandas as pd
import logging

logger = logging.getLogger(__name__)

def add_time_features(df):
    logger.debug('Adding time features...')
    df['transaction_time'] = pd.to_datetime(df['transaction_time'])
    dt = df['transaction_time'].dt
    df['hour'] = dt.hour
    df['year'] = dt.year
    df['month'] = dt.month
    df['day_of_month'] = dt.day
    df['day_of_week'] = dt.dayofweek
    df = df.drop(columns='transaction_time')
    return df


def run_preproc(df):
    df = df.copy()
    df['name'] = df['name_1'] + ' ' + df['name_2']
    df = add_time_features(df)
    df = df.drop(columns=['name_1', 'name_2'])
    return df