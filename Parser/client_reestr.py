"""
Модуль для запроса к серверу и получения сырых данных
"""
import logging
import requests

from requests.exceptions import Timeout, RequestException
from requests.exceptions import JSONDecodeError

from .headers import url as _url
from .headers import headers as _headers
from .headers import json_data as _json_data
from .headers import cols as _cols
from .headers import filter as _filter

from .base_config import BasicConfig

class ReestrParser(BasicConfig):
    """Класс парсера данных о лицензионных участках. 
    Для получения данных используй метод .get_data_from_reestr
    
    Наследует конфигуратор и переменные из класса BasicConfig.
    """

    def __init__(self):
        
        self.logger = logging.getLogger('client')
        
        # Создание объекта сессии и настройка прокси если требуется
        self.session = requests.Session()

        # Проверка конфигурации
        ssl: dict | bool = self.conf.get("SSL", False)
        if ssl:
            self.session.verify = ssl['key']
        else:
            # requests.packages.urllib3.disable_warnings()  # отключить ошибку SSL-сертификата
            self.logger.warning("Отсутствует SSL сертификат. Запрос будет направлен через http без шифрования.")
            self.session.verify = False

        proxy_c: dict | bool = self.conf.get('PROXY', False)
        if proxy_c:
            self.logger.debug("Выполняю подключение через прокси")
            
            prh = proxy_c.get("proxy_host", None)
            prp = proxy_c.get('proxy_port', None)
            pru = proxy_c.get('proxy_user', False)
            prpass = proxy_c.get("proxy_pass", False)

            if pru and prpass:
                self.logger.debug('Логин прокси: %s' % (pru))
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

    def get_records(self, headers: dict, json_data: dict) -> tuple[int, dict]:
        """
        Метод для получения количества записей. Сначала нужен dummy запрос для получения количества записей в реестре
        """

        # Создание запроса
        try:
            self.logger.info('Загружаю данные из реестра')
            response = self.session.post(self.url, headers=headers, json=json_data)
            response = response.json()
            
            numrec = int(response["result"]["recordCount"])
            if numrec > 1: self.logger.info('Данные успешно загружены. Всего строк: %s', numrec)
            
            return numrec, response
        
        except Timeout as te:
            self.logger.error('Ошибка при попытке загрузить данные с сайта. Таймаут. Попробуй настроить прокси в config.ini %s', te, exc_info=False)
            raise
        
        except RequestException as re:
            self.logger.error('Ошибка при попытке загрузить данные с сайта - %s', re, exc_info=False)
            raise
        
        except JSONDecodeError as je:
            self.logger.error('Ошибка десериализации полученного json файла %s', je, exc_info=False)
            self.logger.debug('Строк в json: %s' % (len(response)), exc_info=True)
            raise
        
        except Exception as e:
            self.logger.debug('Ошибка при загрузке данных', exc_info=True)
            self.logger.error('Ошибка при загрузке данных', exc_info=False)
            raise
            
    def get_data_from_reestr(self, filter: str = "oil") -> list:
        """
        Метод делает запросы к базе данных Роснедр.
        Возращает плоский список словарей со значениями сток.
        """
        # Переменная фильтра для запроса
        self.filter: str = _filter(filter)

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
        self.logger.debug(f"Json распарсен - Всего {len(data)} строк")
        return data
