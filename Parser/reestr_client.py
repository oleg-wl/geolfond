#!venv/bin/python3
# -*- coding=utf-8 -*-

"""
Модуль для запроса к серверу и получения сырых данных
"""

import requests

from .headers import url as _url
from .headers import headers as _headers 
from .headers import json_data as _json_data
from .headers import cols as _cols
from .headers import filter as _filter

from .reestr_config import config_path, create_config
from .reestr_config import config_logger, parser_logger

class ReestrRequest:
    """Создание объекта данных из реестра Роснедр https://rfgf.ru/ReestrLic/"""

    def __init__(self):

        # Создание объекта сессии и настройка прокси если требуется
        self.session = requests.Session()

        # Получение ключей config.ini
        self.config = create_config(config_path)
        self.logger = config_logger('client')

        if self.config.has_section('SSL'):
            s = 'SSL'
            self.session.verify = self.config.get(s, 'key')
        else: 
            #requests.packages.urllib3.disable_warnings()  # отключить ошибку SSL-сертификата
            self.logger.warning('Отсутствует SSL сертификат')
            self.session.verify = False

        if self.config.has_section('PROXY'):
            s = 'PROXY'
            prh = self.config.get(s, 'proxy_host')
            prp = self.config.get(s, 'proxy_port')
            pru = self.config.get(s, 'proxy_user')
            prpass = self.config.get(s, 'proxy_pass')

            if pru is not None and prpass is not None:
                self.session.proxies = {"https": f"socks5://{pru}:{prpass}@{prh}:{prp}", 'http':f'socks5://{pru}:{prpass}@{prh}:{prp}'}
            #: Только СОКС5. Для ssh -D 
            else: self.session.proxies = {"https": f"socks5://{prh}:{prp}", 'http':f'socks5://{prh}:{prp}'}

        # Переменные для запроса
        self.url: str = _url
        self.headers: dict = _headers

        # Подстановка нужного фильтра в POST запрос
        self.json_data: dict = _json_data

        # Количество записей в запросе. Для получения всех записей надо делать тестовый запрос
        # Полученное количество записей подставить в сдлвать для следующего запроса
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = 1



    def get_records(self, headers: dict, json_data: dict) -> int | dict:
        """
        Метод для получения количества записей. Сначала нужен dummy запрос для получения количества записей в реестре
        """

        #Создание запроса
        response = self.session.post(
            self.url, headers=headers, json=json_data
        )

        response = response.json()
        numrec = int(response["result"]["recordCount"])

        return numrec, response
    
    @parser_logger('client')
    def get_data_from_reestr(self, filter: str = "oil") -> list:
        """
        Метод делает запросы к базе данных Роснедр.
        Возращает плоский Python-словарь с данными.
        """
        # Переменная фильтра для запроса
        self.filter = _filter(filter) 

        #: Добпавление значения фильтра в заголовок запроса
        self.json_data["RawOlapSettings"]["measureGroup"]["filters"][0][0][
            "selectedFilterValues"
        ] = [self.filter[1]]

        #: Добавление количества записей в заголовок запроса
        nr = self.get_records(headers=self.headers, json_data=self.json_data)
        self.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"] = nr[0]
        
        #: Получить данные запросу с nr записей
        response = self.get_records(headers=self.headers, json_data=self.json_data
        )[1]

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
        self.logger.info(f'Данные загружены успешно. Всего {len(data)} строк')
        return data
