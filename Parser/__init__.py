"""
Модуль для парсинга и подготовки данных из реестра Росгеолфонда для загрузки в BI и визуализации
"""
from .reestr_client import ReestrRequest as client 
from .data_transformer import create_df, create_matrix, save_df
from .headers import filter as _filter

__version__ = '1.0.0b'