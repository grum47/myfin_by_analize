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
from sqlalchemy import create_engine, text
from omegaconf import OmegaConf
from datetime import datetime
from tqdm import tqdm

from transliterate import translit

conf = OmegaConf.load('/home/tests/tests/vscode/myfin_by_analize/conf/server/db/postgres.yaml')

engine = create_engine(
    f"postgresql+psycopg2://{conf.postgres.user}:{conf.postgres.password}@{conf.postgres.host}:{conf.postgres.port}/{conf.postgres.dbname}"
    )

# выбираем данные из сырой таблицы
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
print('Row table read success')

# чистим имена банков
df = df.sort_values(['bank_name','date_page'])

df['bank_name'] = df['bank_name'].apply(
    lambda x: translit(
        x.lower().replace(' ', '_').replace('-', '_'),
        language_code='ru',
        reversed=True
        ).replace("'", '')
    )


# функции подсчета серий повышений и понижений цены
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


# функция подсчета статистик по датафреймам
def new_features(list_datarfames, list_periods):
    X = pd.DataFrame()

    for dataframe in list_datarfames:
        dataframe['diff_day'] = -dataframe.iloc[:, 2].diff().fillna(0)
        suffix = dataframe.name
        num_col = 1
        for win in list_periods:  
            
            X[f'diff_mean_{win}_{suffix}'] = dataframe['diff_day'].rolling(window=win-1).mean().fillna(0)

            mean_decay = lambda x: (x * np.power(0.9, np.arange(win)[::-1])).sum()
            X[f'mean_decay_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).apply(mean_decay, raw=True)

            diff_fl = lambda x: x[0] - x[-1]
            X[f'diff_fl_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).apply(diff_fl, raw=True)

            X[f'mean_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).mean().fillna(0)
            X[f'median_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).median().fillna(0)
            X[f'min_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).min().fillna(0)
            X[f'max_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).max().fillna(0)
            X[f'std_{win}_{suffix}'] = dataframe.iloc[:, num_col].rolling(window=win).std().fillna(0)

    return X


# очищаем витрину
with engine.connect() as conn:
    conn.exec_driver_sql("DROP TABLE myfin.mart_for_model")
    conn.commit()
print('Mart table mart_for_model drop success')

# формируем датафрейм по каждому банку 
print('Start counting bank statistics')
for bank_name in tqdm(df['bank_name'].drop_duplicates().values.tolist()):    
    df_bank = df.loc[df['bank_name'] == bank_name].copy()
    df_bank.name = 'price_usd_sell'

    df_bank.set_index('date_page', inplace=True)
    
    # считаем серии повышений и понижений цены
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

    # считаем статистики
    list_periods = [5, 7, 14, 21, 28, 35, 60, 100]
    df_new_features = new_features([df_bank,], list_periods)
    df_bank = pd.concat([df_bank, df_new_features], axis=1)

    # запись в базу
    df_bank.to_sql(name='mart_for_model', con=engine, schema='myfin', if_exists='append')
    
print(f'Create mart for model success {datetime.now()}')
