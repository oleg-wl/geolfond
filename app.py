#!venv/bin/python
# -*- coding=utf-8 -*-

import requests
import sqlite3
import os
import sys

# requests.packages.urllib3.disable_warnings() #отключить ошибку SSL-сертификата

# Файл с данными для запроса к сайту
from src import queries
from src.panda import construct_df, construct_pivot, save_in_excel, save_in_sql


class ReestrRequest(object):
    """Создание объекта для парсинга данных с реестра Роснедр https://rfgf.ru/ReestrLic/"""

    def __init__(self, filt: str):
        # Создание объекта сессии:
        # Путь к chain-сертификату сайта:
        cert = os.path.relpath("src/key.pem", os.getcwd())

        self.session = requests.Session()
        self.session.verify = cert

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


def path_to_desktop():
    # Функция для определения пути сохранения файла на рабочий стол в зависимости от ОС

    ostype = sys.platform
    if "win" in ostype:
        desktop = os.path.join(os.path.join(os.environ["USERPROFILE"]), "Desktop")
    else:
        desktop = os.path.expanduser("~")
    return desktop


dpath = path_to_desktop()


class PivotMatrix(ReestrRequest):
    # Класс объекта, который преобразуется в pivot для создания матрицы год-прерыдущая лицензия

    def __init__(self, filt):
        super().__init__(filt)

    def pivoting(self):
        self.create_df()

        df = self.dataframe.reset_index()
        self.piv_df = construct_pivot(df)

        return self.piv_df

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

    #filepath = os.path.join(dpath, "reestr_pivot.xlsx")

    reestr = ReestrRequest(queries.lfilt["oil"])
    save_in_excel('data.xlsx', reestr.create_df(), "reestr_oil")

