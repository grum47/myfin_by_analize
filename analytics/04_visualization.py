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


# add write logging to file
logging.basicConfig(
    level=logging.INFO,
    filename="../logs/logs_analytics.log",
    filemode="a",
    format="%(asctime)s: %(levelname)s: %(message)s"
)
logging.info('----------------------------------------------')
folder_name = date.today().strftime('%Y-%m-%d')

if not os.path.exists(f'./report/{folder_name}'):
    os.makedirs(f'./report/{folder_name}')
    logging.info(f"Папка {folder_name} успешно создана")
else:
    logging.info(f"Папка {folder_name} уже существует")


try:
    conf = OmegaConf.load('/home/tests/tests/vscode/myfin_by_analize/conf/server/db/postgres.yaml')
    engine = create_engine(
        f"postgresql+psycopg2://{conf.postgres.user}:{conf.postgres.password}@{conf.postgres.host}:{conf.postgres.port}/{conf.postgres.dbname}"
        )

    logging.info(" :::   Read dynamics data")
    bank = 'nbrb'
    with open('./sql/myfin_dm_read_for_viz_dynamics.sql', 'r') as query:
        df = pd.read_sql_query(query.read(), engine.connect(), parse_dates={'date_page':'%Y-%m-%d'})
    engine.connect().close()

    df_yr_dynamics = df[['date_page', 'price_value_usd_sell', 'mean_14_price_usd_sell', 'mean_28_price_usd_sell']].set_index('date_page').copy()

    # we draw the price dynamics on different windows
    for days in [365, 180, 90, 60]:
        end_dt = df_yr_dynamics.index.max()
        start_dt = end_dt - timedelta(days=days)
        df_dynamics_show = df_yr_dynamics[f"{start_dt}":f"{end_dt}"]

        fig_dynamics = px.line(
            df_dynamics_show,
            x = df_dynamics_show.index, y = 'price_value_usd_sell',
            labels = {
                'price_value_usd_sell': f'',
                'date_page': ''}
        )
        fig_dynamics.add_scatter(x=df_dynamics_show.index.get_level_values(0), y=df_dynamics_show['mean_14_price_usd_sell'], mode='lines')
        fig_dynamics.add_scatter(x=df_dynamics_show.index.get_level_values(0), y=df_dynamics_show['mean_28_price_usd_sell'], mode='lines')

        fig_dynamics.update_layout(
            showlegend=False,
            paper_bgcolor='#f1f1f1',
            plot_bgcolor='#f1f1f1',
            title=f'Динамика курса {days} дней в {bank}',
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
        
        file_name = f"./report/{folder_name}/{len(os.listdir(f'./report/{folder_name}'))}.png"
        fig_dynamics.write_image(f'{file_name}')

    # calculating statistics for cards
    sma_up = 14 if df['is_14_above_28'].values[-1] == 1 else 28
    sma_14_is_up = 1 if df['is_14_up'].values[-1] == 1 else -1
    sma_28_is_up = 1 if df['is_28_up'].values[-1] == 1 else -1

    if sma_14_is_up + sma_28_is_up == 2:
        direct_smas = 'Обе вверх'
    elif sma_14_is_up + sma_28_is_up == 0:
        direct_smas = 'В разные стороны'
    elif sma_14_is_up + sma_28_is_up == -2:
        direct_smas = 'Обе вниз'

    intersect_list = (df['is_14_above_28'] == df['is_14_above_28'].shift(1)).values.tolist()
    for index, value in enumerate(reversed(intersect_list)):
        if value is False:
            intersect_index = len(intersect_list) - index - 1
            break
    intersect_dt = df.iloc[intersect_index, 0]
    intersect_day_ago = (datetime.today() - intersect_dt).days

    df_dynamics_sma_distance = df.loc[df['date_page'] >= datetime.today() - timedelta(days=14)][['date_page', 'abs_distance_btw_14_28']].set_index('date_page').copy()

    def count_up(df, col_name):
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

    df = pd.concat([df, count_up(df[['date_page', 'is_14_up']], 'is_14_up')], axis=1)
    df = pd.concat([df, count_up(df[['date_page', 'is_28_up']], 'is_28_up')], axis=1)
    cnt_day_up_14 = df['cnt_is_14_up'].values[-1]
    cnt_day_up_28 = df['cnt_is_28_up'].values[-1]

    logging.info(" :::   Read last data")
    with open('./sql/myfin_dm_read_for_viz_cards.sql', 'r') as query:
        df = pd.read_sql_query(query.read(), engine.connect(), parse_dates={'date_page':'%Y-%m-%d'}).iloc[0:1]

    date_yesterday = df.iloc[:, 0][0].strftime('%Y-%m-%d')
    bank_name = df.iloc[:, 1][0]
    price_value_usd_sell = df.iloc[:, 2][0]
    price_value_usd_sell_1_day = df.iloc[:, 3][0]
    price_value_usd_sell_7_day = df.iloc[:, 4][0]
    price_value_usd_sell_30_day = df.iloc[:, 5][0]
    price_value_usd_sell_90_day = df.iloc[:, 6][0]
    price_value_usd_sell_365_day = df.iloc[:, 7][0]
    cnt_up = df.iloc[:, 8][0]
    cnt_down = df.iloc[:, 9][0]
    y_predict = float(df.iloc[:, 10][0])

    # cards viz
    fig_cards = go.Figure()
    # 1й столбец
    # Текущий курс валюты
    fig_cards.add_trace(go.Indicator(
        mode = 'number',
        value = price_value_usd_sell,
        number = {'valueformat': '.3f'},
        domain = {'row': 0, 'column': 0}
        )
    )
    # Прогнозный курс на сегодня по модели
    fig_cards.add_trace(go.Indicator(
        title = {'text': f'Прогноз для {bank}'},
        mode = 'number',
        value = y_predict,
        number = {'valueformat': '.3f'},
        domain = {'row': 1, 'column': 0}
        )
    )
    # Количество дней повышений
    fig_cards.add_trace(go.Indicator(
        title = {'text': 'растет'},
        mode = 'number',
        value = cnt_up,
        number = {'valueformat': '.0f', 'suffix': ' дн'},
        domain = {'row': 3, 'column': 0}
        )
    )
    # Количество дней понижений
    fig_cards.add_trace(go.Indicator(
        title = {'text': 'падает'},
        mode = 'number',
        value = cnt_down,
        number = {'valueformat': '.0f', 'suffix': ' дн'},
        domain = {'row': 4, 'column': 0}
        )
    )
    # 2й столбец
    # Курс день ко дню
    fig_cards.add_trace(go.Indicator(
        title = {'text': '1 day'},
        mode = 'delta',
        value = price_value_usd_sell,
        delta = {'position': 'top', 'reference': price_value_usd_sell_1_day, 'relative': True, 'valueformat': '.2%'},
        domain = {'row': 0, 'column': 1}
        )
    )
    # Курс неделя к неделе
    fig_cards.add_trace(go.Indicator(
        title = {'text': '7 day'},
        mode = 'delta',
        value = price_value_usd_sell,
        delta = {'position': 'top', 'reference': price_value_usd_sell_7_day, 'relative': True, 'valueformat': '.2%'},
        domain = {'row': 1, 'column': 1}
        )
    )
    # Курс месяц к месяцу
    fig_cards.add_trace(go.Indicator(
        title = {'text': '30 day'},
        mode = 'delta',
        value = price_value_usd_sell,
        delta = {'position': 'top', 'reference': price_value_usd_sell_30_day, 'relative': True, 'valueformat': '.2%'},
        domain = {'row': 2, 'column': 1}
        )
    )
    # Курс квартал к кварталу
    fig_cards.add_trace(go.Indicator(
        title = {'text': '90 day'},
        mode = 'delta',
        value = price_value_usd_sell,
        delta = {'position': 'top', 'reference': price_value_usd_sell_90_day, 'relative': True, 'valueformat': '.2%'},
        domain = {'row': 3, 'column': 1}
        )
    )
    # Курс год к году
    fig_cards.add_trace(go.Indicator(
        title = {'text': '365 day'},
        mode = 'delta',
        value = price_value_usd_sell,
        delta = {'position': 'top', 'reference': price_value_usd_sell_365_day, 'relative': True, 'valueformat': '.2%'},
        domain = {'row': 4, 'column': 1}
        )
    )
    # 3й столбец
    # когда было пересечение скользащих
    fig_cards.add_trace(go.Indicator(
        title = {'text': f'Пересечение'},
        mode = 'number',
        value = intersect_day_ago,
        number = {'valueformat': '.0f', 'suffix': ' дней назад'},
        domain = {'row': 0, 'column': 2}
        )
    )
    # какая скользящая выше + куда направлены скользящие
    fig_cards.add_trace(go.Indicator(
        title = {'text': f'{direct_smas}'},
        mode = 'number',
        value = sma_up,
        number = {'valueformat': '.0f', 'suffix': ' выше'},
        domain = {'row': 1, 'column': 2}
        )
    )
    # как долго идет вверх скользащая
    fig_cards.add_trace(go.Indicator(
        title = {'text': '14 растет'},
        mode = 'number',
        value = cnt_day_up_14,
        number = {'valueformat': '.0f', 'suffix': ' дн'},
        domain = {'row': 2, 'column': 2}
        )
    )
    fig_cards.add_trace(go.Indicator(
        title = {'text': '28 растет'},
        mode = 'number',
        value = cnt_day_up_28,
        number = {'valueformat': '.0f', 'suffix': ' дн'},
        domain = {'row': 3, 'column': 2}
        )
    )
    # UPDATE LAYOUT
    fig_cards.update_layout(
        title=f'{date_yesterday} USD',
        grid = {'rows': 5, 'columns': 3, 'pattern': 'independent'},
        autosize=False,
        width=650, height=500,
        paper_bgcolor='#f1f1f1',
        plot_bgcolor='#f1f1f1',
        margin={'r': 25, 't': 50, 'l': 25, 'b': 10}
    )
    file_name = f"./report/{folder_name}/{len(os.listdir(f'./report/{folder_name}'))}.png"
    fig_cards.write_image(f'{file_name}')

    # dynamics sma distance
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
            title=f'Динамика дистанции между SMA в {bank}',
            margin={'r': 25, 't': 50, 'l': 25, 'b': 20}
            )
    fig_sma_distance.update_xaxes(
        dtick='M1', showgrid = False
        # rangeslider_visible=True
        )
    fig_sma_distance.update_yaxes(
        showgrid = False
        )
    file_name = f"./report/{folder_name}/{len(os.listdir(f'./report/{folder_name}'))}.png"
    fig_sma_distance.write_image(f'{file_name}')

    logging.info(f" :::   Save img's to {folder_name}")
except Exception as e:
    logging.error(e)
