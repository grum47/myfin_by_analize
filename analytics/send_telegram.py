import os
import time
import telebot

from pathlib import Path
from omegaconf import OmegaConf


class SendTelegram:
    """Класс для отправки сообщений и фотографий в Telegram.

    Этот класс предоставляет методы для отправки сообщений и фотографий в Telegram. 
    Он принимает путь к конфигурационному файлу, который содержит токен и ID чата.
    """
    def __init__(self, conf_path: str):
        self.conf = OmegaConf.load(conf_path)
        self.token = self.conf.telegram.token
        self.chat_id = self.conf.telegram.chat_id

    def send_message(self, message:str) -> bool:
        """Отправляет сообщение в Telegram.

        Этот метод создает экземпляр бота TeleBot с использованием токена, полученного из конфигурационного файла,
        и отправляет сообщение в указанный чат.

        Args:
            message (str): текст сообщения

        Returns:
            bool: True, если сообщение было успешно отправлено
        """
        bot = telebot.TeleBot(token=self.token)
        bot.send_message(chat_id=self.chat_id, text=message)
        return True

    def send_picture(self, path_photo: str) -> bool:
        """Отправляет фотографию в Telegram.

        Этот метод создает экземпляр бота TeleBot с использованием токена, полученного из конфигурационного файла,
        открывает файл фотографии и отправляет его в указанный чат.

        Args:
            path_photo (str): путь к файлу фотографии

        Returns:
            bool: True, если фотография была успешно отправлена
        """
        bot = telebot.TeleBot(token=self.token)
        with open(path_photo, 'rb') as f:
            bot.send_photo(chat_id=self.chat_id, photo=f)
        return True
