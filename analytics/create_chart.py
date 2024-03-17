import pandas as pd
import numpy as np
import psycopg2
import os
import plotly.express as px
import plotly.graph_objects as go
import logging

from datetime import datetime, timedelta, date
from sqlalchemy import create_engine
from omegaconf import OmegaConf

import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter(action='ignore', category=(SettingWithCopyWarning))


class CreateChart:
    def __init__(self, path_conf_db, path_report):
        self.conf = OmegaConf.load(path_conf_db)
        self.user = self.conf.postgres.user
        self.password = self.conf.postgres.password
        self.host = self.conf.postgres.host
        self.port = self.conf.postgres.port
        self.dbname = self.conf.postgres.dbname
        self.path_report = path_report
        self.today_str = date.today().strftime('%Y-%m-%d')
        self.engine = create_engine(
            f"postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )
    
    def read_mart_dynamics(self, path_query_dynamics, path_query_cards):
        with open(path_query_dynamics, 'r') as query:
            df_dynamics = pd.read_sql_query(query.read(), self.engine.connect(), parse_dates={'date_page':'%Y-%m-%d'})

        with open(path_query_cards, 'r') as query:
            df_cards = pd.read_sql_query(query.read(), self.engine.connect(), parse_dates={'date_page':'%Y-%m-%d'}).iloc[0:1]

        return df_dynamics, df_cards
    
    def create_df_dynamics_show(self, df_dynamics):
        df_dynamics_show = df_dynamics[
            [
                'date_page',
                'price_value_usd_sell',
                'mean_14_price_usd_sell',
                'mean_28_price_usd_sell'
            ]
        ].set_index('date_page').copy()
        return df_dynamics_show
    

    def create_chart_dynamic_price(self, df_dynamics_show):
        to_dt = df_dynamics_show.index.max()
        for days in [365, 180, 90, 60]:
            from_dt = to_dt - timedelta(days=days)
            df_dynamics_show = df_dynamics_show[f"{from_dt}":f"{to_dt}"]

            fig_dynamics = px.line(
                df_dynamics_show,
                x = df_dynamics_show.index, y = 'price_value_usd_sell',
                labels = {
                    'price_value_usd_sell': '',
                    'date_page': ''
                    }
            )
            fig_dynamics.add_scatter(x=df_dynamics_show.index.get_level_values(0), y=df_dynamics_show['mean_14_price_usd_sell'], mode='lines')
            fig_dynamics.add_scatter(x=df_dynamics_show.index.get_level_values(0), y=df_dynamics_show['mean_28_price_usd_sell'], mode='lines')

            fig_dynamics.update_layout(
                showlegend=False,
                paper_bgcolor='#f1f1f1',
                plot_bgcolor='#f1f1f1',
                title=f'Динамика курса {days} дней в НБ',
                margin={'r': 25, 't': 50, 'l': 25, 'b': 20},
                autosize=False, width=900, height=350,
                )

            fig_dynamics.update_xaxes(
                dtick='M1', showgrid = False
                # rangeslider_visible=True
                )
            
            fig_dynamics.update_yaxes(
                showgrid = False
                )
            if days <= 60:
                fig_dynamics.update_xaxes(
                    dtick='D1', showgrid = False
                    )
            
            file_name = os.path.join(
                self.path_report,
                self.today_str,
                f'{len(os.listdir(os.path.join(self.path_report, self.today_str)))}.png'
            )
            fig_dynamics.write_image(f'{file_name}')
        return True
    

    def count_up(self, df, col_name: str):
        """
        Функция считает количество строк равных 1
            Таким образом можно вычислить величину серии дней, когда скользящая растет
        Args:
            df (pandas dataframe): Датафрейм
            col_name (string): Имя столбца, чтоб считать по разным столбцам
        Returns:
            pandas dataframe: Датафрейм с новым полем
        """
        cnt_up = 0
        result = []
        for idx, row in df.iterrows():
            if row[f'{col_name}'] == 1:
                cnt_up += 1
            elif row[f'{col_name}'] == 0 and cnt_up > 0:
                cnt_up = 0
            result.append(cnt_up)
        
        df[f'cnt_{col_name}'] = result
        return df[[f'cnt_{col_name}']]


    def calculate_statistics_for_cards(self, df_dynamics, df_cards):
        result = {}
        result['bank'] = 'nbrb'
        # df_dynamics
        result['sma_up'] = 14 if df_dynamics['is_14_above_28'].values[-1] == 1 else 28
        
        sma_14_is_up = 1 if df_dynamics['is_14_up'].values[-1] == 1 else -1
        sma_28_is_up = 1 if df_dynamics['is_28_up'].values[-1] == 1 else -1
        if sma_14_is_up + sma_28_is_up == 2:
            result['direct_smas'] = 'Обе вверх'
        elif sma_14_is_up + sma_28_is_up == 0:
            result['direct_smas'] = 'В разные стороны'
        elif sma_14_is_up + sma_28_is_up == -2:
            result['direct_smas'] = 'Обе вниз'

        intersect_list = (df_dynamics['is_14_above_28'] == df_dynamics['is_14_above_28'].shift(1)).values.tolist()
        for index, value in enumerate(reversed(intersect_list)):
            if value is False:
                intersect_index = len(intersect_list) - index - 1
                break
        intersect_dt = df_dynamics.iloc[intersect_index, 0]
        result['intersect_day_ago'] = (datetime.today() - intersect_dt).days

        df_dynamics_sma_distance = df_dynamics.loc[
            df_dynamics['date_page'] >= datetime.today() - timedelta(days=14)
            ][
                [
                    'date_page',
                    'abs_distance_btw_14_28'
                ]
            ].set_index('date_page').copy()
        
        df_dynamics = pd.concat([df_dynamics, self.count_up(df_dynamics[['date_page', 'is_14_up']], 'is_14_up')], axis=1)
        df_dynamics = pd.concat([df_dynamics, self.count_up(df_dynamics[['date_page', 'is_28_up']], 'is_28_up')], axis=1)
        result['cnt_day_up_14'] = df_dynamics['cnt_is_14_up'].values[-1]
        result['cnt_day_up_28'] = df_dynamics['cnt_is_28_up'].values[-1]

        # df_cards
        result['date_yesterday'] = df_cards.iloc[:, 0][0].strftime('%Y-%m-%d')
        result['bank_name'] = df_cards.iloc[:, 1][0]
        result['price_value_usd_sell'] = df_cards.iloc[:, 2][0]
        result['price_value_usd_sell_1_day'] = df_cards.iloc[:, 3][0]
        result['price_value_usd_sell_7_day'] = df_cards.iloc[:, 4][0]
        result['price_value_usd_sell_30_day'] = df_cards.iloc[:, 5][0]
        result['price_value_usd_sell_90_day'] = df_cards.iloc[:, 6][0]
        result['price_value_usd_sell_365_day'] = df_cards.iloc[:, 7][0]
        result['cnt_up'] = df_cards.iloc[:, 8][0]
        result['cnt_down'] = df_cards.iloc[:, 9][0]
        result['y_predict'] = float(df_cards.iloc[:, 10][0])
        
        return df_dynamics_sma_distance, result
    
    def create_chart_cards(self, result: dict):
        # cards viz
        fig_cards = go.Figure()
        # 1й столбец
        # Текущий курс валюты
        fig_cards.add_trace(go.Indicator(
            mode = 'number',
            value = result['price_value_usd_sell'],
            number = {'valueformat': '.3f'},
            domain = {'row': 0, 'column': 0}
            )
        )
        # Прогнозный курс на сегодня по модели
        fig_cards.add_trace(go.Indicator(
            title = {'text': f'Прогноз для {result["bank"]}'},
            mode = 'number',
            value = result['y_predict'],
            number = {'valueformat': '.3f'},
            domain = {'row': 1, 'column': 0}
            )
        )
        # Количество дней повышений
        fig_cards.add_trace(go.Indicator(
            title = {'text': 'растет'},
            mode = 'number',
            value = result['cnt_up'],
            number = {'valueformat': '.0f', 'suffix': ' дн'},
            domain = {'row': 3, 'column': 0}
            )
        )
        # Количество дней понижений
        fig_cards.add_trace(go.Indicator(
            title = {'text': 'падает'},
            mode = 'number',
            value = result['cnt_down'],
            number = {'valueformat': '.0f', 'suffix': ' дн'},
            domain = {'row': 4, 'column': 0}
            )
        )
        # 2й столбец
        # Курс день ко дню
        # TODO: Переписать на цикл (как в tests.ipynb)
        fig_cards.add_trace(go.Indicator(
            title = {'text': '1 day'},
            mode = 'delta',
            value = result['price_value_usd_sell'],
            delta = {'position': 'top', 'reference': result['price_value_usd_sell_1_day'], 'relative': True, 'valueformat': '.2%'},
            domain = {'row': 0, 'column': 1}
            )
        )
        # Курс неделя к неделе
        fig_cards.add_trace(go.Indicator(
            title = {'text': '7 day'},
            mode = 'delta',
            value = result['price_value_usd_sell'],
            delta = {'position': 'top', 'reference': result['price_value_usd_sell_7_day'], 'relative': True, 'valueformat': '.2%'},
            domain = {'row': 1, 'column': 1}
            )
        )
        # Курс месяц к месяцу
        fig_cards.add_trace(go.Indicator(
            title = {'text': '30 day'},
            mode = 'delta',
            value = result['price_value_usd_sell'],
            delta = {'position': 'top', 'reference': result['price_value_usd_sell_30_day'], 'relative': True, 'valueformat': '.2%'},
            domain = {'row': 2, 'column': 1}
            )
        )
        # Курс квартал к кварталу
        fig_cards.add_trace(go.Indicator(
            title = {'text': '90 day'},
            mode = 'delta',
            value = result['price_value_usd_sell'],
            delta = {'position': 'top', 'reference': result['price_value_usd_sell_90_day'], 'relative': True, 'valueformat': '.2%'},
            domain = {'row': 3, 'column': 1}
            )
        )
        # Курс год к году
        fig_cards.add_trace(go.Indicator(
            title = {'text': '365 day'},
            mode = 'delta',
            value = result['price_value_usd_sell'],
            delta = {'position': 'top', 'reference': result['price_value_usd_sell_365_day'], 'relative': True, 'valueformat': '.2%'},
            domain = {'row': 4, 'column': 1}
            )
        )
        # 3й столбец
        # когда было пересечение скользащих
        fig_cards.add_trace(go.Indicator(
            title = {'text': f'Пересечение'},
            mode = 'number',
            value = result['intersect_day_ago'],
            number = {'valueformat': '.0f', 'suffix': ' дней назад'},
            domain = {'row': 0, 'column': 2}
            )
        )
        # какая скользящая выше + куда направлены скользящие
        fig_cards.add_trace(go.Indicator(
            title = {'text': f'{result["direct_smas"]}'},
            mode = 'number',
            value = result['sma_up'],
            number = {'valueformat': '.0f', 'suffix': ' выше'},
            domain = {'row': 1, 'column': 2}
            )
        )
        # как долго идет вверх скользащая
        fig_cards.add_trace(go.Indicator(
            title = {'text': '14 растет'},
            mode = 'number',
            value = result['cnt_day_up_14'],
            number = {'valueformat': '.0f', 'suffix': ' дн'},
            domain = {'row': 2, 'column': 2}
            )
        )
        fig_cards.add_trace(go.Indicator(
            title = {'text': '28 растет'},
            mode = 'number',
            value = result['cnt_day_up_28'],
            number = {'valueformat': '.0f', 'suffix': ' дн'},
            domain = {'row': 3, 'column': 2}
            )
        )
        # UPDATE LAYOUT
        fig_cards.update_layout(
            title=f'{result["date_yesterday"]} USD',
            grid = {'rows': 5, 'columns': 3, 'pattern': 'independent'},
            autosize=False,
            width=650, height=500,
            paper_bgcolor='#f1f1f1',
            plot_bgcolor='#f1f1f1',
            margin={'r': 25, 't': 50, 'l': 25, 'b': 10}
        )
        file_name = os.path.join(
                self.path_report,
                self.today_str,
                f'{len(os.listdir(os.path.join(self.path_report, self.today_str)))}.png'
            )
        fig_cards.write_image(f'{file_name}')
        return True
    
    def create_chart_sma_dynamics(self, df_dynamics_sma_distance, result):
        fig_sma_distance = px.area(
                df_dynamics_sma_distance,
                x = df_dynamics_sma_distance.index, y = 'abs_distance_btw_14_28',
                labels = {
                    'abs_distance_btw_14_28': f'',
                    'date_page': ''}
            )
        fig_sma_distance.update_layout(
                showlegend=False,
                paper_bgcolor='#f1f1f1',
                plot_bgcolor='#f1f1f1',
                width=400, height=200,
                title=f'Динамика дистанции между SMA в {result["bank"]}',
                margin={'r': 25, 't': 50, 'l': 25, 'b': 20}
                )
        fig_sma_distance.update_xaxes(
            dtick='M1', showgrid = False
            # rangeslider_visible=True
            )
        fig_sma_distance.update_yaxes(
            showgrid = False
            )
        file_name = os.path.join(
                self.path_report,
                self.today_str,
                f'{len(os.listdir(os.path.join(self.path_report, self.today_str)))}.png'
            )
        fig_sma_distance.write_image(f'{file_name}')
        return True