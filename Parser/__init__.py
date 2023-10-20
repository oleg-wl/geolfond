"""
Модуль для парсинга и подготовки данных из реестра Росгеолфонда для загрузки в BI и визуализации
"""
from .reestr_client import ReestrRequest as client 
from .data_sender import EmailSender as sender

from .data_transformer import DataTransformer as transformer
from .data_saver import DataSaver as saver
from .data_kabdt import DataKdemp as kdemp

from .headers import filter as _filter
from .reestr_config import getlogger as _logger

__version__ = '4.0'