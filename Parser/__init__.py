"""
Модуль для парсинга и подготовки данных из реестра Росгеолфонда для загрузки в BI и визуализации
"""
from .reestr_client import ReestrRequest as client 
from .data_sender import EmailSender as sender
from .reestr_config import config_logger

from .data_transformer import DataTransformer as transformer
from .data_saver import DataSaver as saver
from .headers import filter as _filter

__version__ = '4.0'