import os
import shutil
import logging

from configparser import ConfigParser
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Базовый класс для настройки объектов парсера 
# и обработчика 

class BasicConfig:
    basedir = os.path.abspath(os.path.dirname((__file__)))
    
    cnf_path = os.path.join(basedir, "config.ini")
    config_file = ConfigParser()
    config_file.read(cnf_path)
    
    conf: dict = config_file._sections
    
    path: str = config_file.get('default', 'data_folder', fallback = 'data')
    loglevel: int = config_file.getboolean('default', 'debug', fallback=False)
    

    logging.basicConfig(
        format="%(levelname)s - %(asctime)s: %(message)s LINE: (%(lineno)d) in %(name)s",
        datefmt="%x %X", level=logging.WARNING
    )
    if loglevel: logging.basicConfig(level=logging.DEBUG)
    
    logging.getLogger("urllib3").setLevel(level=logging.WARNING)
    logging.getLogger("exchangelib").setLevel(level=logging.WARNING)
    logging.getLogger("spnego").setLevel(level=logging.WARNING)
    logging.getLogger('exchangelib.util.xml').setLevel(level=logging.WARNING)
    
    @classmethod
    def basic_config(cls) -> None:
        """Метод для базовой настройки. Создать нужные папки и скопровать конфиг.ини
        """
        
        #создать папку для сохранения экселей, если нет
        if not os.path.exists(cls.path):
            os.mkdir(cls.path)
            print('%s created'% cls.path)
        else: print('%s exists' %cls.path)
        
        #скопировать пример конфига в конфиг.ини
        if not os.path.exists(cls.cnf_path):
            shutil.copy(os.path.abspath('example.config.ini'), cls.cnf_path)
            print('%s created'% cls.cnf_path)
        else: print('%s exists' %cls.cnf_path)
        print('Config.. OK')