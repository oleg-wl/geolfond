#!venv/bin/python3
# -*- coding=utf-8 -*-

"""
Модуль для запроса к серверу и получения сырых данных
"""

import os
import requests
import socks
import socket

from .queries import url as _url
from .queries import headers as _headers 
from .queries import json_data as _json_data
from .queries import lfilt as _lfilt
from .queries import cols as _cols

class ReestrRequest(object):
    """Создание объекта данных из реестра Роснедр https://rfgf.ru/ReestrLic/"""

    def __init__(self):
        # Переменные для запроса
        self.url = _url
        self.headers = _headers

        # Подстановка нужного фильтра в POST запрос
        self.json_data = _json_data

        # Количество записей в запросе. Для получения всех записей надо делать тестовый запрос
        # Полученное количество записей подставить в сдлвать для следующего запроса
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = 1
        

    def config(self):
        """Запуск конфигурации"""
        
        # Создание объекта сессии:
        self.session = requests.Session()

        # Блок проверки наличия config.ini
        if os.path.exists("src/config.ini"):
            from configparser import ConfigParser

            config = ConfigParser()
            config.read("src/config.ini")

            self.path = os.path.abspath(config["DEFAULT"]["path"])
            self.logfile = os.path.abspath(config["DEFAULT"]["logfile"])
            

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


    def get_record_count(self, filter: str = "oil"):
        """
        Метод для получения количества записей.
        """
        
        self.filter = filter 
        self.filt = _lfilt[self.filter]
        self.json_data["RawOlapSettings"]["measureGroup"]["filters"][0][0][
            "selectedFilterValues"
        ] = [self.filt]

        #Создание запроса
        response = self.session.post(
            self.url, headers=self.headers, json=self.json_data
        ).json()
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = int(
            response["result"]["recordCount"]
        )

    def get_data_from_reestr(self, filter: str = "oil"):
        """
        Метод делает запросы к базе данных Роснедр.
        Возращает плоский Python-словарь с данными.
        """

        # Запрос для получения всех записей выгрузки
        self.get_record_count(filter)
        response = self.session.post(
            self.url, headers=self.headers, json=self.json_data
        ).json()
        #return response.text

        # Подготовка данных
        response["result"]["data"]["cols"][16] = ["Дата.1"]
        response["result"]["data"]["cols"][18] = ["Дата.2"]

        cols = [x.replace(k, v) for x in [v[0] for v in response["result"]["data"]["cols"]] for k, v in _cols.items() if x == k]
        vals = response["result"]["data"]["values"]

        #Возващает словарь с данными 
        self.data =  dict(zip(cols, vals))
        return self.data

