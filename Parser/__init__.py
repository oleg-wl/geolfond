"""
Модуль для парсинга и подготовки данных из реестра Росгеолфонда для загрузки в BI и визуализации
"""
from .client.reestr import ReestrParser as reestr
from .client.multiplier import MultiplParser as multipl

from .datatr.sender import EmailSender as sender

from .datatr.transformer import DataTransformer as transformer
from .datatr.saver import DataSaver as saver
from .datatr.kabdt import DataKdemp as kdemp

from .headers import filter as _filter

import logging

logging.basicConfig(
    format="%(levelname)s - %(asctime)s: %(message)s LINE: (%(lineno)d) in %(name)s",
    datefmt="%x %X",
    level=logging.INFO,
)
logging.getLogger("urllib3").setLevel(level=logging.INFO)
logging.getLogger("exchangelib").setLevel(level=logging.INFO)
logging.getLogger("spnego").setLevel(level=logging.INFO)
logging.getLogger('exchangelib.util.xml').setLevel(level=logging.WARNING)

__version__ = '5.0'
