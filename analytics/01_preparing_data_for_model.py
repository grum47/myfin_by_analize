# Признаки для модели:
# - цена продажи банком на конец дня
# - спред между продажей и покупкой на конец дня
# - количество подряд значений вверх
# - количество подряд значений вниз
# - изменение цены относительно предыдущего значения
# - значение по периодам
#     - среднее изменение цены за период
#     - средневзвешенное экспоненциальное сглаживание цены за период
#     - изменение цены относително начального значения в периоде
#     - среднее значение цены за период
#     - медиана значения цены за период
#     - минимальное значение цены за период
#     - максимальное значение цены за период
#     - взвешенное среднее значение цены за период

import pandas as pd
import numpy as np
import psycopg2
import logging

from sqlalchemy import create_engine
from sqlalchemy import text
from transliterate import translit
from omegaconf import OmegaConf


# add write logging to file
logging.basicConfig(
    level=logging.INFO,
    filename="../logs/logs_analytics.log",
    filemode="a",
    format="%(asctime)s: %(levelname)s: %(message)s"
)

# read configuration
conf = OmegaConf.load('/home/tests/tests/vscode/myfin_by_analize/conf/server/db/postgres.yaml')
# create db engine
engine = create_engine(
    f"postgresql+psycopg2://{conf.postgres.user}:{conf.postgres.password}@{conf.postgres.host}:{conf.postgres.port}/{conf.postgres.dbname}"
    )

logging.info("Read raw data")
query =    """
    select  
            date_page
            , bank_name
            , price_value_usd_sell
            , price_value_usd_buy - price_value_usd_sell as bank_spred
    from 	myfin.myfin.myfin_raw mr 
    -- and date_page >= current_date - '1000 day'::interval
    """

df = pd.read_sql(query, engine.connect(), parse_dates={'date_page':'%Y-%m-%d'})
logging.info('Read raw data success')

# clear bank names
df = df.sort_values(['bank_name','date_page'])

df['bank_name'] = df['bank_name'].apply(
    lambda x: translit(
        x.lower().replace(' ', '_').replace('-', '_'),
        language_code='ru',
        reversed=True
        ).replace("'", '')
    )
logging.info('Clear bank names success')

# functions for calculating series of price up and down
def count_up(df):
    cnt_up = 0
    result = []
    for idx, row in df.iterrows():
        if row['is_up'] == 1:
            cnt_up += 1
        elif row['is_up'] in [0, -1] and cnt_up > 0:
            cnt_up = 0
        result.append(cnt_up)
    
    df['cnt_up'] = result
    return df


def count_down(df):
    cnt_down = 0
    result = []
    for idx, row in df.iterrows():
        if row['is_up'] == -1:
            cnt_down += 1
        elif row['is_up'] in [0, 1] and cnt_down > 0:
            cnt_down = 0
        result.append(cnt_down)
    
    df['cnt_down'] = result
    return df


# the function of calculating statistics on dataframes
def new_features(list_datarfames, list_periods):
    X = pd.DataFrame()

    for dataframe in list_datarfames:
        dataframe['diff_day'] = -dataframe.iloc[:, 2].diff().fillna(0)
        suffix = dataframe.name
        num_col = 1
        for win in list_periods:  
            
            mean_decay = lambda x: (x * np.power(0.9, np.arange(win)[::-1])).sum()
            diff_fl = lambda x: x[0] - x[-1]

            X[f'diff_mean_{win}_{suffix}'] = dataframe['diff_day'].rolling(window=win-1).mean().fillna(0)
            X[f'mean_decay_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).apply(mean_decay, raw=True)
            X[f'diff_fl_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).apply(diff_fl, raw=True)
            X[f'mean_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).mean().fillna(0)
            X[f'median_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).median().fillna(0)
            X[f'min_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).min().fillna(0)
            X[f'max_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).max().fillna(0)
            X[f'std_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).std().fillna(0)

    return X


# cleaning the mart
with engine.connect() as conn:
    conn.exec_driver_sql("DROP TABLE myfin.mart_for_model")
    conn.commit()
logging.info('Mart table mart_for_model drop success')

logging.info('Start counting bank statistics')
for bank_name in df['bank_name'].drop_duplicates().values.tolist():
    df_bank = df.loc[df['bank_name'] == bank_name].copy()
    df_bank.name = 'price_usd_sell'

    df_bank.set_index('date_page', inplace=True)
    
    # we consider a series of price up and down
    df_bank['is_up'] = np.where(
        (df_bank['price_value_usd_sell'] - df_bank['price_value_usd_sell'].shift(1)) > 0,
        1, np.where(
            (df_bank['price_value_usd_sell'] - df_bank['price_value_usd_sell'].shift(1)) < 0,
            -1, 0
            )
        )
    df_bank = count_up(df_bank)
    df_bank = count_down(df_bank)
    df_bank.drop('is_up', axis=1, inplace=True)

    # counting statistics by windows
    list_periods = [5, 7, 14, 21, 28, 35, 60, 100]
    df_new_features = new_features([df_bank,], list_periods)
    df_bank = pd.concat([df_bank, df_new_features], axis=1)
    df_bank.dropna(inplace=True)
    df_bank['y'] = df_bank['price_value_usd_sell'].shift(-1)
    df_bank['y_predict'] = None

    # writing to the database
    df_bank.to_sql(name='mart_for_model', con=engine, schema='myfin', if_exists='append')


logging.info('Create mart for model success')
