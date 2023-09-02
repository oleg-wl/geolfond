
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

cf_path = 'config.ini' #NOTE: можно изменить путь к конфиг файлу
basedir = os.path.abspath(os.path.dirname((__file__)))
config_path = os.path.join(basedir, cf_path)

def create_config(path: os.path = config_path) -> ConfigParser:
    """
    Базовая функция для создания конфига

    :param str cf_path: путь к конфиг ини, defaults to 'config.ini'
    :raises ValueError: если нет файла
    :return _type_: объект конфигпарсера
    """

    if os.path.exists(path):

        config_file = ConfigParser()
        config_file.read(path)
    else: raise ValueError(f'Неверный путь к конфиг файлу')

    return config_file

#:functions for check default paths 
def check_path() -> os.PathLike:
    c = create_config(config_path)
    p = c.get('PATHS','data_folder', fallback=None)
    return p if p is not None else os.path.abspath('data')

def check_logfile():
    c = create_config(config_path)
    p = c.get('PATHS', 'logfile', fallback=None)
    return p if p is not None else os.path.abspath('log./logfile.log')

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
                raise j_err
            except Timeout as conn_err:
                logger.error(f'Timeout error: {conn_err}. Доступ только из РФ. Добавь [PROXY] в config.ini')
                raise conn_err
            except RequestException as conn_err:
                logger.error(f'Ошибка подключения {conn_err}')
                raise conn_err
            except Exception as e:
                logger.exception(f'{e}')
                raise e
                
            return result
        return wrapper
    return added_logger

class EmptyFolder(Exception):
    def __init__(self) -> Exception:
        super().__init__(self)

    def __str__(self) -> str:
        return f'EmptyFolder: в папке нет файлов'


        