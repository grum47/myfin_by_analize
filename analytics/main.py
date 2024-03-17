import os
import time
import logging

from datetime import date
from pathlib import Path

from transform_data_for_model import TransformData
from learning_model import LearningModel
from get_predict import GetPredict
from create_chart import CreateChart
from send_telegram import SendTelegram

path_main = os.path.dirname(__file__)
project_path = os.path.dirname(path_main)

path_conf_tg = os.path.join(project_path, "conf/server/telegram/telegram.yaml")
path_conf_pg = os.path.join(project_path, "conf/server/db/postgres.yaml")
path_logs = os.path.join(project_path, "logs/logs_analytics.log")

path_models = os.path.join(path_main, "models")
path_report = os.path.join(path_main, "report")
path_sql = os.path.join(path_main, "sql")

today_str = date.today().strftime('%Y-%m-%d')

logging.basicConfig(
    level=logging.INFO,
    filename=path_logs,
    filemode="a",
    format="%(asctime)s: %(levelname)s: %(message)s"
)


def create_folder(path, today_str):
    path_folder = os.path.join(path, today_str)
    if not os.path.exists(path_folder):
        os.makedirs(path_folder)
        logging.info(f"Папка {path_folder} успешно создана")
    else:
        logging.info(f"Папка {path_folder} уже существует")
    return True


def step_01(currency: str, path_query: str, mart_name: str, bank_name: str) -> bool:
    """Первый шаг обработки данных.

    Эта функция выполняет первый шаг обработки данных, который включает в себя создание движка базы данных,
    чтение сырых данных из базы данных, очистку имен банков, очистку таблицы в базе данных и вычисление статистики.

    Args:
        currency (str): валюта, для которой нужно обработать данные
        path_query (str): путь к SQL-запросу для чтения данных
        mart_name (str): имя таблицы в базе данных, в которую нужно сохранить результаты
        bank_name (str): имя банка, по которому нужен расчет (будет реализовано в одной из следующих версий)

    Returns:
        bool: True, если все шаги были успешно выполнены
    """
    transform = TransformData(path_conf_pg)
    engine = transform.create_db_engine()
    logging.info('  :::   Try read raw data')
    try:
        df = transform.read_raw_data(engine, path_query)
        df = transform.clear_bank_names(df)
    except Exception as e:
        logging.error(e)
    
    logging.info('  :::   Try clean mart')
    try:
        transform.clean_mart(engine, mart_name)
    except Exception as e:
        logging.error(e)

    logging.info('  :::   Try calculate statistics and save mart')
    try:
        transform.calculate_statistics(engine, df, bank_name, mart_name)
    except Exception as e:
        logging.error(e)
    return True


def step_02(currency: str, path_query: str) -> int:
    """Второй шаг обработки данных.

    Эта функция выполняет второй шаг обработки данных, который включает в себя чтение данных из таблицы
    в базе данных, обучение модели и сохранение модели.

    Args:
        currency (str): валюта, для которой нужно обработать данные
        path_query (str): путь к SQL-запросу для чтения данных из таблицы в базе данных

    Returns:
        int: оценка качества модели
    """
    create_folder(path_models, today_str)
    learning = LearningModel(path_conf_pg)
    engine = learning.create_db_engine()

    logging.info('  :::   Try read mart data')
    try:
        df = learning.read_mart(engine, path_query)
    except Exception as e:
        logging.error(e)

    logging.info('  :::   Try learning model')
    try:
        pipeline, score = learning.learning_model(df)
    except Exception as e:
        logging.error(e)

    logging.info('  :::   Try save model')   
    try: 
        learning.save_model(pipeline, path_models)
    except Exception as e:
        logging.error(e)
    return score


def step_03(currency: str, path_query: str, mart_name) -> bool:
    """Третий шаг обработки данных.

    Эта функция выполняет третий шаг обработки данных, который включает в себя чтение данных из таблицы
    в базе данных, загрузку модели, создание прогнозов и обновление базы данных.

    Args:
        currency (str): валюта, для которой нужно обработать данные
        path_query (str): путь к SQL-запросу для чтения данных из таблицы в базе данных
        path_models (str): путь к папке с сохраненными моделями

    Returns:
        bool: True, если все шаги были успешно выполнены
    """
    predict = GetPredict(path_conf_pg)
    logging.info('  :::   Try read mart data')
    try:
        df_test, date_page = predict.read_mart(path_query)
    except Exception as e:
        logging.error(e)
    
    logging.info('  :::   Try load model and save predict')
    try:
        model = predict.load_model(path_models)
        prediction = predict.make_predictions(model, df_test)
        predict.update_db(date_page, prediction, mart_name)
    except Exception as e:
        logging.error(e)
    return True


def step_04(currency: str, path_query_dynamics: str, path_query_cards: str) -> bool:
    """Четвертый шаг обработки данных.

    Эта функция выполняет четвертый шаг обработки данных, который включает в себя чтение данных из таблицы
    в базе данных, создание графиков и диаграмм на основе этих данных.

    Args:
        currency (str): валюта, для которой нужно обработать данные
        path_query_dynamics (str): путь к SQL-запросу для чтения данных для графиков динамики
        path_query_cards (str): путь к SQL-запросу для чтения данных для карточек

    Returns:
        bool: True, если все шаги были успешно выполнены
    """
    vis = CreateChart(path_conf_pg, path_report)
    create_folder(path_report, today_str)

    logging.info('  :::   Try read marts')
    try:
        df_dynamics, df_cards = vis.read_mart_dynamics(path_query_dynamics, path_query_cards)
        df_dynamics_show = vis.create_df_dynamics_show(df_dynamics)
    except Exception as e:
        logging.error(e)
    
    logging.info('  :::   Try create chart')
    try:
        vis.create_chart_dynamic_price(df_dynamics_show)
        logging.info('  :::   Create chart dynamics')
        df_dynamics_sma_distance, result = vis.calculate_statistics_for_cards(df_dynamics, df_cards)
        vis.create_chart_cards(result)
        logging.info('  :::   Create chart cards')
        vis.create_chart_sma_dynamics(df_dynamics_sma_distance, result)
        logging.info('  :::   Create chart dynamics sma')
    except Exception as e:
        logging.error(e)
    return True


def step_05(currency: str, path_folder) -> bool:
    """Пятый шаг обработки данных.

    Эта функция выполняет пятый шаг обработки данных, который включает в себя отправку графиков и диаграмм
    в Telegram.

    Args:
        currency (str): валюта, для которой нужно обработать данные

    Returns:
        bool: True, если все шаги были успешно выполнены
    """
    bot = SendTelegram(path_conf_tg)
    logging.info('  :::   Try send pictures')
    try:
        for fl in sorted(os.listdir(path_folder)):
            path_pic = Path(os.path.join(path_folder, fl))
            bot.send_picture(path_pic)
            time.sleep(0.31)
            logging.info(f'{path_pic} send')
    except Exception as e:
        logging.error(e)
    return True


if __name__ == "__main__":
    for currency in ['blr']:
        logging.info(f'---------------------------------------------- {currency} analyze {today_str}')

        # step 1: transform data for model
        step_01(
            currency,
            path_query=(os.path.join(path_sql, currency, "01_read_raw_data.sql")),
            bank_name='nbrb',
            mart_name='myfin_by'
        )

        # step 2: model training
        logging.info(f'---------------------------------------------- {currency} model training')
        step_02(
            currency,
            path_query=(os.path.join(path_sql, currency, "02_read_mart.sql"))
        )
        
        # step 3: predict
        logging.info(f'---------------------------------------------- {currency} predict')
        step_03(
            currency,
            path_query=os.path.join(path_sql, currency, "03_read_mart.sql"),
            mart_name='myfin_by'
            )
        
        # step 4: visualization 
        logging.info(f'---------------------------------------------- {currency} visualization')
        step_04(
            currency,
            path_query_dynamics=os.path.join(path_sql, currency, "04_read_mart_dynamics.sql"),
            path_query_cards=os.path.join(path_sql, currency, "05_read_mart_cards.sql")
            )

        # step 5: send telegram
        logging.info(f'---------------------------------------------- {currency} send telegram')
        step_05(
            currency,
            os.path.join(path_report, today_str)
        )
