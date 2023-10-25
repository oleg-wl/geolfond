import os
from configparser import ConfigParser
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Базовый класс для настройки объектов парсера 
# и обработчика 

class BasicConfig:
    basedir = os.path.abspath(os.path.dirname((__file__)))
    config_file = ConfigParser()
    config_file.read(os.path.join(basedir, "config.ini"))
    
    conf: dict = config_file._sections
    
    try:
        path = conf['PATHS']['data_folder']
    except KeyError:
        path = 'data'
    
    @classmethod
    def basic_config(cls):
    
        if not os.path.exists(cls.path):
            os.mkdirs(cls.path)
            
        
