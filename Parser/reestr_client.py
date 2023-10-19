#!venv/bin/python3
# -*- coding=utf-8 -*-

"""
Модуль для запроса к серверу и получения сырых данных
"""

import requests
import re
from bs4 import BeautifulSoup as bs
import datetime
from io import StringIO

from .headers import url as _url
from .headers import headers as _headers
from .headers import json_data as _json_data
from .headers import cols as _cols
from .headers import filter as _filter
from .headers import headers_price as _hp
from .headers import headers_price_duty as _hpd
from .headers import url_smtb as _urlsmtb
from .headers import url_fas as _urlfas

from .reestr_config import config_path, create_config
from .reestr_config import add_logger, logger


class ReestrRequest:
    """Создание объекта данных из реестра Роснедр https://rfgf.ru/ReestrLic/"""

    def __init__(self):
        # Создание объекта сессии и настройка прокси если требуется
        self.session = requests.Session()

        # Получение ключей config.ini
        self.config = create_config(config_path)
        self.logger = logger()

        if self.config.has_section("SSL"):
            s = "SSL"
            self.session.verify = self.config.get(s, "key")
        else:
            # requests.packages.urllib3.disable_warnings()  # отключить ошибку SSL-сертификата
            self.logger.warning("Отсутствует SSL сертификат")
            self.session.verify = False

        if self.config.has_section("PROXY"):
            s = "PROXY"
            prh = self.config.get(s, "proxy_host")
            prp = self.config.get(s, "proxy_port")
            pru = self.config.get(s, "proxy_user")
            prpass = self.config.get(s, "proxy_pass")

            if pru is not None and prpass is not None:
                self.session.proxies = {
                    "https": f"socks5h://{pru}:{prpass}@{prh}:{prp}",
                    "http": f"socks5h://{pru}:{prpass}@{prh}:{prp}",
                }
            #: Только СОКС5. Для ssh -D
            else:
                self.session.proxies = {
                    "https": f"socks5://{prh}:{prp}",
                    "http": f"socks5://{prh}:{prp}",
                }

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

        # Создание запроса
        response = self.session.post(self.url, headers=headers, json=json_data)

        response = response.json()
        numrec = int(response["result"]["recordCount"])

        return numrec, response

    @add_logger
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
        response = self.get_records(headers=self.headers, json_data=self.json_data)[1]

        # Подготовка данных
        response["result"]["data"]["cols"][16] = ["Дата.1"]
        response["result"]["data"]["cols"][18] = ["Дата.2"]

        cols = [
            x.replace(k, v)
            for x in [v[0] for v in response["result"]["data"]["cols"]]
            for k, v in _cols.items()
            if x == k
        ]
        vals = response["result"]["data"]["values"]

        # Плоский список словарей-строк в которых столбец:значение
        data: list = [
            {key: vals[n][i] for n, key in enumerate(cols)} for i in range(len(vals[0]))
        ]

        #: добавление столбца с фильтром
        for i in data:
            i["filter"] = self.filter[1]

        # Возващает список словарей-строк и фильтр
        self.logger.info(f"Данные загружены успешно. Всего {len(vals)} строк")
        return data

    def get_currency(
        self, start_date: str, end_date: str = None, today: bool = True
    ) -> dict:
        """Метод для получения среднемесячного курса доллара для расчета коэф Р в Кц НДПИ
        NOTE: Формат дат для подстановки в запрос - дд.мм.гггг

        Args:
            start_date (str): Начальная дата для подстановки в GET-запрос в формате дд.мм.гггг
            end_date (str): Конечная дата для подстановки в GET-запрос в формате дд.мм.ГГГ (Dafault = None)
            taday (bool): Default True выбор текущей даты (сегодня). Если False то укажи end_date

        Returns:
            dict: python словарь. Ключ Date - значение торгового дня; ключ Rate - значение средней ставки ЦБ РФ в этот день
        """
        if today == True:
            end_date = datetime.datetime.strftime(datetime.datetime.now(), "%d.%m.%Y")
        elif end_date == None:
            self.logger.error("Если today=False, укажи end_date")
            raise ValueError

        pat = r"\b(0[1-9]|[1-2]\d|3[0-1])\.(0[1-9]|1[0-2])\.\d{4}\b"
        if not re.fullmatch(pat, start_date) or not re.fullmatch(pat, end_date):
            self.logger.error("Неверный формат даты. Нвдо дд.мм.гггг")
            raise ValueError(f"{start_date}{end_date}")
        else:
            url = f"https://cbr.ru/currency_base/dynamics/?UniDbQuery.Posted=True&UniDbQuery.so=1&UniDbQuery.mode=1&UniDbQuery.date_req1=&UniDbQuery.date_req2=&UniDbQuery.VAL_NM_RQ=R01235&UniDbQuery.From={start_date}&UniDbQuery.To={end_date}"
            r = self.session.get(url=url)
            self.logger.info("Загружаю средний курс ЦБ РФ")

            raw = bs(r.text, "html.parser")
            table_tag = raw.table

            rows = [i for i in table_tag.stripped_strings][5:]
            data = {
                "Dates": [v for n, v in enumerate(rows) if n % 3 == 0],
                "Rate": [rows[n + 2] for n, v in enumerate(rows) if n % 3 == 0],
            }

            return data

    def get_oil_price(self, rng: int = 7) -> dict[datetime.datetime, str]:
        """
        Метод для парсинга цен на нефть с сайта Минэкономразвития. Публикуемые котировки Argus для расчета коэфициента Кц.

        :return dict: словаь с ценами
        """

        url1: str = "https://economy.gov.ru/material/departments/d12/konyunktura_mirovyh_tovarnyh_rynkov/?type=&page="
        url2: str = "https://economy.gov.ru"
        headers: dict = _hp

        dates: list = []
        prices: list = []

        pat_dt = re.compile("(?P<date>\w* \d{4})")
        pat_price = re.compile("(?P<usd>\d{1,3},\d{1,2})")

        self.logger.info("Загружаю котировки Argus")
        for counts in range(1, rng):
            resp = self.session.get(url=url1 + str(counts), headers=headers)

            data = bs(resp.text, "html.parser")

            for i in data.find_all("a", attrs={"title": re.compile("Юралс")}):
                link = i["href"]

                raw = self.session.get(url=url2 + link, headers=headers)
                html_data = bs(raw.text, "html.parser")

                txt = html_data.get_text(strip=True)

                dt = re.findall(pat_dt, txt)[1]
                dates.append(dt)

                pr = re.findall(pat_price, txt)[0]
                prices.append(pr)

        #: Переименовать месяц для объекта datetime
        dates_fixed = []
        for item in dates:
            for old, new in [
                ("январь", "01"),
                ("февраль", "02"),
                ("март", "03"),
                ("апрель", "04"),
                ("май", "05"),
                ("июнь", "06"),
                ("июль", "07"),
                ("август", "08"),
                ("сентябрь", "09"),
                ("октябрь", "10"),
                ("ноябрь", "11"),
                ("декабрь", "12"),
            ]:
                if old in item:
                    d = datetime.datetime.strptime(item.replace(old, new), "%m %Y")
                    dates_fixed.append(d)

        return {"Dates": dates_fixed, "Price": prices}

    def get_abdt_index(self) -> dict:
        #: Требуемые индексы захардкодил.
        ind = ["reg", "dtl", "dtm", "dtz"]
        d = {}
        try:
            self.logger.info("Загружаю цены с СПБ биржи")
            for index in ind:
                r = self.session.get(_urlsmtb.format(index=index))
                d[index] = StringIO(r.text)

            return d
        except Exception as e:
            self.logger.error("Ошибка загрузки цен с биржи")
            self.logger.debug(e)
            raise

    def get_oilprice_monitoring(self):
        """Метод для получения данных Р в ЭП (Средняя цена юралс в период мониторинга)
        Возвращает два инстанса класса
        date - str - html-таблица с данными
        dt - str - дата публикации данных на сайте
        Returns:
            self
        """

        # Основная ссылка для запроса
        try:
            self.logger.info("Загружаю цены Юралс и Брент в мониторинге")
            url1 = "https://www.economy.gov.ru/material/directions/vneshneekonomicheskaya_deyatelnost/tamozhenno_tarifnoe_regulirovanie/"
            resp = self.session.get(url=url1, headers=_hpd).text

            # Найти последнюю ссылку на публикацию и ее дату
            page = bs(resp, "html.parser")
            link = page.find(
                "a", attrs={"title": re.compile("вывозных таможенных пошлин на нефть")}
            )
            # Найти дату публикации
            patt = "(\d{2} \w+ \d{4})"
            date = re.findall(patt, str(link.span))

            # Извлечь последнюю часть ссылки для добавления к ссылке и перехода на страницу с данными
            url2 = link["href"].rsplit(sep="/")[-1]
            resp2 = self.session.get(url=url1 + url2, headers=_hpd).text
            page2 = bs(resp2, "html.parser")
            result = page2.find("table")

            # html строка с таблицей для добавления
            self.data = str(result)
            self.dt = "".join(date)  # апак листа
            return self

        except:
            self.logger.error(f"Ошибка при парсинге сайта минэка")
            self.logger.debug(resp)

    def get_fas_akciz(self) -> str:
        self.logger.info("Загружаю цены ФАС")

        sess = requests.Session()
        r = sess.get(url=_urlfas)
        d = bs(r.text, "html.parser")

        return [x for x in d.find_all("table")]
