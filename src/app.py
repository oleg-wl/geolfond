#!venv/bin/python
# -*- coding=utf-8 -*-

import requests
import socks
import socket
import os

import src.queries as queries


class ReestrRequest(object):
    """Создание объекта данных из реестра Роснедр https://rfgf.ru/ReestrLic/"""

    def __init__(self, filter: str='oil'):
        # Создание объекта сессии:
        self.filter = filter

    def config(self):
        self.filt = queries.lfilt[self.filter]
        self.session = requests.Session()

        # Блок проверки наличия config.ini
        if os.path.exists("config.ini"):
            from configparser import ConfigParser

            config = ConfigParser()
            config.read("config.ini")

            self.path = os.path.abspath(config['DEFAULT']['path'])

            # Настройки для прокси через российский VDS
            if "PROXY" in config:
                proxy_host = config["PROXY"]["proxy_host"]
                proxy_port = config["PROXY"]["proxy_port"]

                socks.set_default_proxy(socks.SOCKS5, proxy_host, proxy_port)
                socket.socket = socks.socksocket
                self.session.proxies = {"https": f"socks5://{proxy_host}:{proxy_port}"}

            # Настройка SSL
            if "SSL" in config:
                cert = os.path.relpath(config["SSL"]["key"], os.getcwd())
                self.session.verify = cert
        else:
            requests.packages.urllib3.disable_warnings()  # отключить ошибку SSL-сертификата
            self.path = os.getcwd()

        # Переменные для запроса
        self.url = queries.url
        self.headers = queries.headers

        # Подстановка нужного фильтра в POST запрос
        self.json_data = queries.json_data
        self.json_data["RawOlapSettings"]["measureGroup"]["filters"][0][0][
            "selectedFilterValues"
        ] = [self.filt]

        # Количество записей в запросе. Для получения всех записей надо делать тестовый запрос
        # Полученное количество записей подставить в сдлвать для следующего запроса
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = 1

    def get_record_count(self):
        """
        Метод для получения количества записей.
        """
        response = self.session.post(
            self.url, headers=self.headers, json=self.json_data
        ).json()
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = int(
            response["result"]["recordCount"]
        )

    def get_data_from_reestr(self):
        """
        Метод делает запросы к базе данных Роснедр.
        Возращает плоский Python-словарь с данными.
        """

        # Запрос для получения всех записей выгрузки
        self.get_record_count()
        response = self.session.post(
            self.url, headers=self.headers, json=self.json_data
        ).json()

        # Подготовка данных
        response["result"]["data"]["cols"][16] = ["Дата.1"]
        response["result"]["data"]["cols"][18] = ["Дата.2"]

        cols = [v[0] for v in response["result"]["data"]["cols"]]
        vals = response["result"]["data"]["values"]
        self.data = dict(zip(cols, vals))





