"""
Модуль для парсинга и подготовки данных из реестра Росгеолфонда для загрузки в BI и визуализации
"""
from .reestr_client import ReestrRequest as _client 
from .data_transformer import create_df, create_matrix

client = _client()
client.config()

   
__version__ = '2.0.0'