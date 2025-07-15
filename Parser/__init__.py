"""
Модуль для парсинга и подготовки данных из реестра Росгеолфонда для загрузки в BI и визуализации
"""
from .client.reestr import ReestrParser as reestr
from .client.multiplier import MultiplParser as multipl

from .datatr.sender import EmailSender as sender

from .datatr.transformer import DataTransformer as transformer
from .datatr.saver import DataSaver as saver
from .datatr.kabdt import DataKdemp as kdemp

from .base_config import BasicConfig as _conf
from .headers import filter as _filter


__version__ = '5.0'
