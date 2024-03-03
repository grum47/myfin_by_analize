import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from omegaconf import OmegaConf
import logging

from joblib import load


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
try:
    query =    """
        select  *
        from 	myfin.myfin.mart_for_model mr 
        where   bank_name = 'nbrb'
        and     y is null
        """
    df = pd.read_sql(query, engine.connect())
    engine.connect().close()

    df = df.loc[df['bank_name'] == 'nbrb'].reset_index(drop=True).copy()
    date_page = df.at[0, 'date_page'].strftime('%Y-%m-%d')
    X_test = df.drop(['date_page', 'bank_name', 'y', 'y_predict'], axis=1).copy()

    logging.info(" :::   Load Model")
    model = load('zion19.joblib')

    predictions = round(model.predict(X_test)[0][0], 4)
    logging.info(f" :::   Predictions: {predictions}")

    with engine.connect() as conn:
        query = f"UPDATE myfin.mart_for_model SET y_predict = {predictions} where date_page = '{date_page}'"
        logging.info(query)
        conn.exec_driver_sql(query)
        conn.commit()

    logging.info(f" :::   Predictions write to DB")
except Exception as e:
    logging.error(e)