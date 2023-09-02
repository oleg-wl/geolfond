
""" 
Конфигуратор из файла конфигурации config.ini
Также функции пути для папки с данными и конфигами 
Функция для создания объекта логгинга и декоратор для логгирования ошибок при загрузке данных
"""
import os
from functools import wraps
import logging
from sys import stdout
from requests.exceptions import JSONDecodeError, Timeout, RequestException

from configparser import ConfigParser

def create_config(cf_path='config.ini'):
    basedir = os.path.abspath(os.path.dirname((__file__)))
    config_path = os.path.join(basedir, cf_path)

    if os.path.exists(config_path):

        config_file = ConfigParser()
        config_file.read(config_path)
    else: raise ValueError(f'Неверный путь к конфиг файлу')

    return config_file

class ReestrConfig:

    def __init__(self):

        self.config_proxy = None
        self.proxy_auth = None
        self.config_ssl = None

            config_file = ConfigParser()
            config_file.read(self.config_path)
        
            os.environ['DATA_FOLDER_PATH'] = os.path.abspath(config_file["DEFAULT"]["data_folder"])

            os.environ['LOGFILE'] = os.path.abspath(config_file["DEFAULT"]["logfile"])

            

            # Настройки для прокси через российский VDS
            if "PROXY" in config_file:
                proxy_host = config_file["PROXY"]["proxy_host"]
                proxy_port = config_file["PROXY"]["proxy_port"]
                proxy_user = config_file["PROXY"]["proxy_user"]
                proxy_pass = config_file["PROXY"]["proxy_pass"]

                self.config_proxy = {"https": f"socks5://{proxy_host}:{proxy_port}"}

                if proxy_user != 'None':
                    self.proxy_auth = (proxy_user, proxy_pass)
                
            # Настройка SSL
            if "SSL" in config_file:
                cert = os.path.relpath(config_file["SSL"]["key"], os.getcwd())
                self.config_ssl = cert

            if "email" in config_file:
                self.smtp_server = config_file['email']['smtp_server']
                self.smtp_port = config_file['email']['smtp_port']
                self.smtp_email = config_file['email']['smtp_user']
                self.smtp_password = config_file['email']['smtp_password']
                self.smtp_to = config_file['email']['smtp_to']
            

#:functions for check default paths 
def check_path() -> os.PathLike:
    return os.environ['DATA_FOLDER_PATH'] if os.environ.get('DATA_FOLDER_PATH') is not None else os.path.abspath('data')

def check_logfile():
    return os.environ['LOGFILE'] if os.environ.get('LOGFILE') is not None else os.path.abspath('log/defaultlog.log')

def config_logger(name: str = __name__):
    """
    Функция возвращает объект Logger для инициализации логгера
    Debug level пишет в файл, указанный в конфиге
    Info level пишет в консоль

    :param str name: название логгера
    :return _type_: объект логгера
    """
    #logging.basicConfig()

    logger = logging.getLogger(name=name)
    logger.setLevel(logging.DEBUG)

    fh_form = logging.Formatter('%(levelname)s, %(asctime)s: %(message)s (LINE: (%(lineno)d)' , datefmt='%x %X')
    ch_form = logging.Formatter('[%(name)s]: %(message)s', datefmt='%x %X')

    fh = logging.FileHandler(check_logfile(), mode='a', encoding='utf-8')
    fh.setLevel(logging.DEBUG) #Логи в файл для отправки по почте
    fh.setFormatter(fh_form)

    ch = logging.StreamHandler(stream=stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(ch_form)

    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger

#:Декоратор для логгинга ошибок в консольной версии
def parser_logger(logger_name: str = __name__):

    def added_logger(func):
        @wraps(func)
        def wrapper(self, filter: str):
            logger = config_logger(logger_name)

            try:
                result = func(self, filter)
                logger.info('Данные загружены успешно')
            except JSONDecodeError as j_err:
                logger.error(f'Вернулся пустой JSON: {j_err}')
                raise
            except Timeout as conn_err:
                logger.error(f'Timeout error: {conn_err}. Доступ только из РФ. Добавь [PROXY] в config.ini')
                raise
            except RequestException as conn_err:
                logger.error(f'Ошибка подключения {conn_err}')
                raise
            except Exception as e:
                logger.exception(f'{e}')
                raise
                
            return result
        return wrapper
    return added_logger

class EmptyFolder(Exception):
    def __init__(self) -> Exception:
        super().__init__(self)

    def __str__(self) -> str:
        return f'EmptyFolder: в папке нет файлов'


        