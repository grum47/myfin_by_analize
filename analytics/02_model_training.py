import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from omegaconf import OmegaConf
import logging
import os
from datetime import date

from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression

from joblib import dump


# add write logging to file
logging.basicConfig(
    level=logging.INFO,
    filename="../logs/logs_analytics.log",
    filemode="a",
    format="%(asctime)s: %(levelname)s: %(message)s"
)
logging.info('----------------------------------------------')

conf = OmegaConf.load('/home/tests/tests/vscode/myfin_by_analize/conf/server/db/postgres.yaml')
engine = create_engine(
    f"postgresql+psycopg2://{conf.postgres.user}:{conf.postgres.password}@{conf.postgres.host}:{conf.postgres.port}/{conf.postgres.dbname}"
    )

logging.info(" :::   Read raw data")
query =    """
    select  *
    from 	myfin.myfin.mart_for_model mr 
    where   date_page >= '2022-01-01'
    and     bank_name = 'nbrb'
    and     y is not null
    order by date_page
    """
df = pd.read_sql(query, engine.connect(), parse_dates={'date_page':'%Y-%m-%d'})
engine.connect().close()

logging.info(" :::   Create X_train, y_train")
df_X_train = df.drop(columns=['date_page', 'bank_name', 'y', 'y_predict'], axis=1).copy()
df_y_train = df[['y']].copy()
# Learning model
for bank in df.bank_name.drop_duplicates().values.tolist():
    if bank == 'nbrb':
        logging.info(f"BANK {bank}")

        tasks = [
            ('scaler', StandardScaler()),
            ('classifier', LinearRegression())
        ]

        pipeline = Pipeline(tasks)

        tscv = TimeSeriesSplit(n_splits=5)

        for train_index, test_index in tscv.split(df_X_train):

            logging.info(f"TRAIN: {train_index.min()} - {train_index.max()}, TEST: {test_index.min()} - {test_index.max()}")
            
            X_train, X_test = df_X_train.iloc[train_index], df_X_train.iloc[test_index]
            y_train, y_test = df_y_train.iloc[train_index], df_y_train.iloc[test_index]

            pipeline.fit(X_train, y_train)

            score = pipeline.score(X_test, y_test)
            logging.info(f"Accuracy: {score}")

logging.info(" :::   Learning model success")

folder_name = date.today().strftime('%Y-%m-%d')

if not os.path.exists(f'./models/{folder_name}'):
    os.makedirs(f'./models/{folder_name}')
    logging.info(f"Папка {folder_name} успешно создана")
else:
    logging.info(f"Папка {folder_name} уже существует")

dump(pipeline, f'./models/{folder_name}/zion17.joblib')
logging.info(f" :::   Save model to {folder_name}")
