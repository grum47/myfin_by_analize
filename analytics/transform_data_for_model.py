import psycopg2
import numpy as np
import pandas as pd

from datetime import date
from omegaconf import OmegaConf
from transliterate import translit
from sqlalchemy import create_engine

class TransformData:
    def __init__(self, path_conf_db):
        self.conf = OmegaConf.load(path_conf_db)
        self.user = self.conf.postgres.user
        self.password = self.conf.postgres.password
        self.host = self.conf.postgres.host
        self.port = self.conf.postgres.port
        self.dbname = self.conf.postgres.dbname
        self.today_str = date.today().strftime('%Y-%m-%d')

    def create_db_engine(self):
        """Создает и возвращает экземпляр движка базы данных.

        Эта функция создает экземпляр движка базы данных, используя соединение с базой данных,
        указанное в атрибутах объекта. Она принимает строку соединения, которая включает в себя
        имя пользователя, пароль, хост, порт и имя базы данных.

        Returns:
            engine: экземпляр движка базы данных
        """
        engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )
        return engine

    def read_raw_data(self, engine, path_query):
        with open(path_query, 'r') as query:
            df = pd.read_sql_query(query.read(), engine.connect(), parse_dates={'date_page': '%Y-%m-%d'})
        return df

    def clear_bank_names(self, df):
        """Очищает и нормализует имена банков в DataFrame.

        Эта функция принимает DataFrame и нормализует имена банков, заменяя пробелы на подчеркивания,
        дефисы на подчеркивания и приводя их к нижнему регистру. Затем она применяет обратный перевод
        текста с использованием русского языка и удаляет апострофы.

        Args:
            df (DataFrame): DataFrame, который нужно очистить

        Returns:
            DataFrame: DataFrame с очищенными именами банков
        """
        df = df.sort_values(['bank_name', 'date_page'])
        df['bank_name'] = df['bank_name'].apply(
            lambda x: translit(
                x.lower().replace(' ', '_').replace('-', '_'),
                language_code='ru',
                reversed=True
            ).replace("'", '')
        )
        return df

    def count_up(self, df):
        """Считает количество последовательных положительных значений в столбце 'is_up' DataFrame.

        Эта функция принимает DataFrame и создает новый столбец 'cnt_up', который содержит количество
        последовательных положительных значений в столбце 'is_up'. Если значение в столбце 'is_up'
        равно 0 или -1 и предыдущие значения были положительными, счетчик сбрасывается.

        Args:
            df (DataFrame): DataFrame, в котором нужно посчитать количество последовательных положительных значений

        Returns:
            DataFrame: DataFrame с новым столбцом 'cnt_up'
        """
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

    def count_down(self, df):
        """Считает количество последовательных отрицательных значений в столбце 'is_up' DataFrame.

        Эта функция принимает DataFrame и создает новый столбец 'cnt_down', который содержит количество
        последовательных отрицательных значений в столбце 'is_up'. Если значение в столбце 'is_up'
        равно 0 или 1 и предыдущие значения были положительными, счетчик сбрасывается.

        Args:
            df (DataFrame): DataFrame, в котором нужно посчитать количество последовательных отрицательных значений

        Returns:
            DataFrame: DataFrame с новым столбцом 'cnt_down'
        """
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

    def new_features(self, list_datarfames, list_periods):
        """Добавляет новые признаки в DataFrame.

        Эта функция принимает список DataFrame и список периодов. Для каждого DataFrame в списке,
        она добавляет новые признаки, основанные на разнице между соседними значениями, скользящем
        среднем, скользящем медиане, скользящем минимуме, скользящем максимуме, скользящем стандартном
        отклонении и скользящем среднем с экспоненциальным затуханием.

        Args:
            list_datarfames (list): список DataFrame, в которых нужно добавить новые признаки
            list_periods (list): список периодов для скользящих окон

        Returns:
            DataFrame: DataFrame с новыми признаками
        """
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

    def clean_mart(self, engine, mart_name):
        with engine.connect() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS myfin_dm.{mart_name}")
            conn.commit()
        return True

    def calculate_statistics(self, engine, df, bank_name, mart_name):
        """Вычисляет статистику для каждого банка и сохраняет ее в базе данных.

            Эта функция принимает движок базы данных, DataFrame с данными о банках, имя банка и имя таблицы
            в базе данных. Она проходит по каждому банку в DataFrame, вычисляет статистику и сохраняет ее
            в базе данных.

        Args:
            df (DataFrame): DataFrame с данными о банках
            bank_name (str): имя банка, для которого нужно вычислить статистику
            mart_name (str): имя таблицы в базе данных, в которую нужно сохранить статистику

        Returns:
            bool: True, если статистика была успешно сохранена
        """
        for bank in df['bank_name'].drop_duplicates().values.tolist():
            if bank == bank_name:
                df_bank = df.loc[df['bank_name'] == bank].copy()
                df_bank.name = 'price_usd_sell'
                df_bank.set_index('date_page', inplace=True)
                df_bank['is_up'] = np.where(
                    (df_bank['price_value_usd_sell'] - df_bank['price_value_usd_sell'].shift(1)) > 0,
                    1, np.where((df_bank['price_value_usd_sell'] - df_bank['price_value_usd_sell'].shift(1)) < 0, -1, 0))
                df_bank = self.count_up(df_bank)
                df_bank = self.count_down(df_bank)
                df_bank.drop('is_up', axis=1, inplace=True)

                list_periods = [5, 7, 14, 21, 28, 35, 60, 100]
                df_new_features = self.new_features([df_bank, ], list_periods)
                df_bank = pd.concat([df_bank, df_new_features], axis=1)
                df_bank.dropna(inplace=True)
                df_bank['y'] = df_bank['price_value_usd_sell'].shift(-1)
                df_bank['y_predict'] = None
                df_bank.to_sql(name=mart_name, con=engine, schema='myfin_dm', if_exists='append')
        return True
