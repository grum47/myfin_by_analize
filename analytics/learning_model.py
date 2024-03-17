import psycopg2
import pandas as pd

from joblib import dump
from datetime import date
from omegaconf import OmegaConf
from sqlalchemy import create_engine

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import TimeSeriesSplit



class LearningModel:
    def __init__(self, path_conf_db):
        self.conf = OmegaConf.load(path_conf_db)
        self.user = self.conf.postgres.user
        self.password = self.conf.postgres.password
        self.host = self.conf.postgres.host
        self.port = self.conf.postgres.port
        self.dbname = self.conf.postgres.dbname
        self.today_str = date.today().strftime('%Y-%m-%d')

    def create_db_engine(self):
        engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )
        return engine
    
    def read_mart(self, engine, path_query):
        with open(path_query, 'r') as query:
            df = pd.read_sql_query(query.read(), engine.connect(), parse_dates={'date_page': '%Y-%m-%d'})
        return df

    def learning_model(self, df):
        df_X_train = df.drop(columns=['date_page', 'bank_name', 'y', 'y_predict'], axis=1).copy()
        df_y_train = df[['y']].copy()
        tasks = [
            ('scaler', StandardScaler()),
            ('classifier', LinearRegression())
        ]
        pipeline = Pipeline(tasks)
        """
        TODO:
        Так как происходит обучение на всем датасете, то можно убрать кросс валидацию
        """
        tscv = TimeSeriesSplit(n_splits=5)
        for train_index, test_index in tscv.split(df_X_train):            
            X_train, X_test = df_X_train.iloc[train_index], df_X_train.iloc[test_index]
            y_train, y_test = df_y_train.iloc[train_index], df_y_train.iloc[test_index]
            pipeline.fit(X_train, y_train)
            score = pipeline.score(X_test, y_test)
        return pipeline, score
    

    def save_model(self, pipeline, path_models):
        model_name = 'zion17.joblib'
        dump(pipeline, f'{path_models}/{self.today_str}/{model_name}')
        return True
