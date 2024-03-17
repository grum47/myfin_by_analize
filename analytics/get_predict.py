import psycopg2
import numpy as np
import pandas as pd

from joblib import load
from datetime import date
from omegaconf import OmegaConf
from sqlalchemy import create_engine


class GetPredict:
    def __init__(self, path_conf_db):
        self.conf = OmegaConf.load(path_conf_db)
        self.user = self.conf.postgres.user
        self.password = self.conf.postgres.password
        self.host = self.conf.postgres.host
        self.port = self.conf.postgres.port
        self.dbname = self.conf.postgres.dbname
        self.today_str = date.today().strftime('%Y-%m-%d')
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )

    def read_mart(self, path_query):
        with open(path_query, 'r') as query:
            df = pd.read_sql_query(query.read(), self.engine.connect())

        df = df.reset_index(drop=True).copy()
        date_page = df.at[0, 'date_page'].strftime('%Y-%m-%d')
        X_test = df.drop(['date_page', 'bank_name', 'y', 'y_predict'], axis=1).copy()
        return X_test, date_page

    def load_model(self, path_models):
        model_name = 'zion17.joblib'
        model = load(f'{path_models}/{self.today_str}/{model_name}')
        return model

    def make_predictions(self, model, X_test):
        prediction = round(model.predict(X_test)[0][0], 4)
        return prediction

    def update_db(self, date_page, prediction, mart_name):
        with self.engine.connect() as conn:
            query = f"UPDATE myfin_dm.{mart_name} SET y_predict = {prediction} where date_page = '{date_page}'"
            conn.exec_driver_sql(query)
            conn.commit()
        return True
