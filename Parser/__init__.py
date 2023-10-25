"""
Модуль для парсинга и подготовки данных из реестра Росгеолфонда для загрузки в BI и визуализации
"""
from .client_reestr import ReestrParser as reestr
from .client_multiplier import MultiplParser as multipl

from .data_sender import EmailSender as sender

from .data_transformer import DataTransformer as transformer
from .data_saver import DataSaver as saver
from .data_kabdt import DataKdemp as kdemp

from .headers import filter as _filter

import logging

logging.basicConfig(
    format="%(levelname)s - %(asctime)s: %(message)s LINE: (%(lineno)d) in %(name)s",
    datefmt="%x %X",
    level=logging.DEBUG,
)
logging.getLogger("urllib3").setLevel(level=logging.INFO)
logging.getLogger("exchangelib").setLevel(level=logging.INFO)
logging.getLogger("spnego").setLevel(level=logging.INFO)
logging.getLogger('exchangelib.util.xml').setLevel(level=logging.WARNING)

__version__ = '4.0'