#!venv/bin/python3
# -*- coding=utf-8 -*-

"""
Модуль для запроса к серверу и получения сырых данных
"""

from multiprocessing import context
import requests

from .headers import url as _url
from .headers import headers as _headers 
from .headers import json_data as _json_data
from .headers import cols as _cols
from .headers import filter as _filter

from .reestr_config import ReestrConfig

class ReestrRequest:
    """Создание объекта данных из реестра Роснедр https://rfgf.ru/ReestrLic/"""

    def __init__(self):

        _conf = ReestrConfig()

        # Создание объекта сессии и настройка прокси если требуется
        self.session = requests.Session()

        if _conf.config_proxy is not None:
            self.session.proxies = _conf.config_proxy

        if _conf.proxy_auth is not None:
            self.session.auth = _conf.proxy_auth

        #requests.packages.urllib3.disable_warnings()  # отключить ошибку SSL-сертификата
        if _conf.config_ssl is not None:
            self.session.verify = _conf.config_ssl
        else: self.session.verify = False 
        
        # Переменные для запроса
        self.url: str = _url
        self.headers: dict = _headers

        # Подстановка нужного фильтра в POST запрос
        self.json_data: dict = _json_data

        # Количество записей в запросе. Для получения всех записей надо делать тестовый запрос
        # Полученное количество записей подставить в сдлвать для следующего запроса
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = 1


    def get_record_count(self):
        """
        Метод для получения количества записей.
        """

        #Создание запроса
        response = self.session.post(
            self.url, headers=self.headers, json=self.json_data
        ).json()
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = int(
            response["result"]["recordCount"]
        )

    def get_data_from_reestr(self, filter: str = "oil") -> list:
        """
        Метод делает запросы к базе данных Роснедр.
        Возращает плоский Python-словарь с данными.
        """
        # Переменная фильтра для запроса
        self.filter = _filter(filter) 
        
        self.json_data["RawOlapSettings"]["measureGroup"]["filters"][0][0][
            "selectedFilterValues"
        ] = [self.filter[1]]

        # Запрос для получения всех записей выгрузки
        self.get_record_count()
        response = self.session.post(
            self.url, headers=self.headers, json=self.json_data
        ).json()

        # Подготовка данных
        response["result"]["data"]["cols"][16] = ["Дата.1"]
        response["result"]["data"]["cols"][18] = ["Дата.2"]

        cols = [x.replace(k, v) for x in [v[0] for v in response["result"]["data"]["cols"]] for k, v in _cols.items() if x == k]
        vals = response["result"]["data"]["values"]

        # Плоский список словарей-строк в которых столбец:значение
        data: list = [{key:vals[n][i] for n, key in enumerate(cols)} for i in range(len(vals[0]))]
        
        #: добавление столбца с фильтром
        for i in data:
            i['filter'] = self.filter[1]

        #Возващает список словарей-строк и фильтр
        return data
