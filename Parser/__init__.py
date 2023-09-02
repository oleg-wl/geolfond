"""
Модуль для парсинга и подготовки данных из реестра Росгеолфонда для загрузки в BI и визуализации
"""
from .reestr_client import ReestrRequest as client 
from .data_sender import EmailSender as sender
from .reestr_config import config_logger

from .data_transformer import create_df, create_matrix, save_df
from .headers import filter as _filter

__version__ = '2.1.0'