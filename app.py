#!venv/bin/python
# -*- coding=utf-8 -*-

import requests
import socks
import sqlite3
import os

# requests.packages.urllib3.disable_warnings() #отключить ошибку SSL-сертификата

if os.path.exists("config.ini"):
    from configparser import ConfigParser

    config = ConfigParser()
    config.read("config.ini")

    # Настройки для прокси через российский VDS
    if "PROXY" in config:
        proxy_host = config["PROXY"]["proxy_host"]
        proxy_port = config["PROXY"]["proxy_port"]

        socks.set_default_proxy(socks.SOCKS5, proxy_host, proxy_port)
        socket = socks.socksocket



# Файл с данными для запроса к сайту
from src import queries
from src.panda import construct_df, save_in_excel, save_in_sql


class ReestrRequest(object):
    """Создание объекта для парсинга данных с реестра Роснедр https://rfgf.ru/ReestrLic/"""

    def __init__(self, filt: str):
        # Создание объекта сессии:
        # Путь к chain-сертификату сайта:
        cert = os.path.relpath("src/key.pem", os.getcwd())

        self.session = requests.Session()
        self.session.verify = cert
        if "socket" in locals() or "socket" in globals():
            self.session.proxies = {"https": f"socks5://{proxy_host}:{proxy_port}"}

        # Переменная фильтра
        self.filt = filt

        self.url = queries.url
        self.headers = queries.headers

        # Подстановка нужного фильтра в POST запрос
        self.json_data = queries.json_data
        self.json_data["RawOlapSettings"]["measureGroup"]["filters"][0][0][
            "selectedFilterValues"
        ] = [filt]

        self.nested_data = {}

    def get_record_count(self):
        """
        Метод для получения количества записей в базе данных Роснедр для создания запроса на полное количество данных.
        Возвращает int с количеством записей в базе для использования в методе get_data
        """

        # Данные для запроса
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = 1

        response = self.session.post(
            self.url, headers=self.headers, json=self.json_data
        ).json()
        return int(response["result"]["recordCount"])

    def get_data_from_reestr(self):
        """
        Метод делает запросы к базе данных Роснедр.
        Возращает плоский Python-словарь с данными.
        """

        # Получение количества записей в реестре. Создание заголовка запроса для получения всех записей выгрузки
        num = self.get_record_count()
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = num

        # Запрос для получения всех записей выгрузки
        response = self.session.post(
            self.url, headers=self.headers, json=self.json_data
        ).json()

        # Подготовка данных
        response["result"]["data"]["cols"][16] = ["Дата.1"]
        response["result"]["data"]["cols"][18] = ["Дата.2"]

        # Python-словарь из данных запроса для загрузки его в панду или сохранения
        self.nested_data = {
            k: v
            for n, v in enumerate(response["result"]["data"]["values"])
            for k in response["result"]["data"]["cols"][n]
        }
        return self.nested_data

    def create_df(self):
        """
        Метод создает пандасовский датафрейм из выгруженного словаря self.nested_data
        """
        self.dataframe = construct_df(self.get_data_from_reestr())
        return self.dataframe


class ReestrDatabase(ReestrRequest):
    # Класс для сохранения в базу данных sqlite3

    def __init__(self, filt: str):
        super().__init__(filt)
        self.filt = filt
        name = [
            k for k, v in queries.lfilt.items() if self.filt == v
        ]  # извлечение ключа словаря фильтра для названия таблицы

        # Create connection to DB
        conn = sqlite3.Connection("reestr_database.db", detect_types=1)

        # Request and create dataframe
        self.create_df()

        save_in_sql(*name, self.dataframe, conn)


if __name__ == "__main__":
    # Запуск для автоматической выгрузки по нефти или дебаггинга

    # filepath = os.path.join(dpath, "reestr_pivot.xlsx")

    reestr = ReestrRequest(queries.lfilt["oil"])
    save_in_excel(
        "/var/www/lordcrabov.ru/public_html/data.xlsx", reestr.create_df(), "reestr_oil"
    )
