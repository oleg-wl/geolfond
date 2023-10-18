
""" 
Конфигуратор из файла конфигурации config.ini
Также функции пути для папки с данными и конфигами 
Функция для создания объекта логгинга и декоратор для логгирования ошибок при загрузке данных
"""
import os
from functools import wraps
import logging
from requests.exceptions import JSONDecodeError, Timeout, RequestException

from configparser import ConfigParser

cf_path = 'config.ini' #NOTE: можно изменить путь к конфиг файлу
basedir = os.path.abspath(os.path.dirname((__file__)))
config_path = os.path.join(basedir, cf_path)


logging.basicConfig(format='%(levelname)s - %(asctime)s: %(message)s LINE: (%(lineno)d) in %(name)s' , datefmt='%x %X', level=logging.INFO)
def logger():
    return logging.getLogger(name=__name__)

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


#:Декоратор для логгинга ошибок в консольной версии
def add_logger(func):
    @wraps(func)
    def wrapper(self, filter: str):
        
        try:
            result = func(self, filter)
            return result
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
            
    return wrapper

def basic_logging(msg: str , error: str, logger_name: str = __name__):
    """
    Декоратор для логгинга сообщений и ошибок с помощью объекта логгера. Инфо в консоль, Дебаг в файл.
    Добавляй декоратор к функциям в main или geolfond модулях

    :param str msg: Сообщение для успеха
    :param str error: Собщение для ошибки
    :param str logger_name: название логгера, defaults to __name__
    """
    def add_logger(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            
            try:
                result = func(*args, **kwargs)

                #Сообщение успех
                logger.info(msg=msg)
            except Exception as e:
                logger.error(msg=error)
                logger.debug(msg=error, exc_info=e)
            return result
        return wrapper
    return add_logger

def send_only_me(send_message):
    @wraps(send_message)
    def sender(*args, **kwargs):
        r = send_message(*args, **kwargs)
        del r.message['To']
        r.message['To'] = r.smtp_to[-1]
        return r
    return sender
                
class EmptyFolder(Exception):
    def __init__(self) -> Exception:
        super().__init__(self)

    def __str__(self) -> str:
        return f'EmptyFolder: в папке нет файлов'


        