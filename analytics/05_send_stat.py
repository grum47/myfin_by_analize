import telebot
from omegaconf import OmegaConf
import logging 
import os
from datetime import date
import time
from pathlib import Path


# add write logging to file
logging.basicConfig(
    level=logging.INFO,
    filename="../logs/logs_analytics.log",
    filemode="a",
    format="%(asctime)s: %(levelname)s: %(message)s"
)
logging.info('----------------------------------------------')

try:
    conf = OmegaConf.load('../conf/server/telegram/telegram.yaml')

    token = conf.telegram.token
    chat_id = conf.telegram.chat_id
    folder_name = date.today().strftime('%Y-%m-%d')


    def send_message_tg(message):
        token = conf.telegram.token
        chat_id = conf.telegram.chat_id
        bot = telebot.TeleBot(token=token)
        bot.send_message(chat_id=chat_id, text=message)

        return True


    def send_picture_tg(path_photo):
        token = conf.telegram.token
        chat_id = conf.telegram.chat_id
        bot = telebot.TeleBot(token=token)
        with open(path_photo, 'rb') as f:
            bot.send_photo(chat_id=chat_id, photo=f)

        return True

    for fl in sorted(os.listdir(f'./report/{folder_name}')):
        path_img = os.path.dirname(os.path.abspath(__file__)) + f'/report/{folder_name}/{fl}'
        send_picture_tg(path_img)
        time.sleep(0.3)
        logging.info(f'{path_img} send')

except Exception as e:
    logging.error(e)
