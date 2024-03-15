import os
import shutil

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
    
    try:
        path = conf['PATHS']['data_folder']
    except KeyError:
        path = 'data'
    
    @classmethod
    def basic_config(cls) -> None:
        """Метод для базовой настройки. Создать нужные папки и скопровать конфиг.ини
        """
        
        #создать папку для сохранения экселей, если нет
        if not os.path.exists(cls.path):
            os.mkdirs(cls.path)
            print('%s created'% cls.path)
        else: print('%s exists' %cls.path)
        
        #скопировать пример конфига в конфиг.ини
        if not os.path.exists(cls.cnf_path):
            shutil.copy(os.path.abspath('example.config.ini'), cls.cnf_path)
            print('%s created'% cls.cnf_path)
        else: print('%s exists' %cls.cnf_path)
        print('Config.. OK')