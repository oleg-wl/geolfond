import os
from configparser import ConfigParser


class BasicConfig:
    basedir = os.path.abspath(os.path.dirname((__file__)))
    config_file = ConfigParser().read(os.path.join(basedir, "config.ini"))
    conf = config_file._sections
    
    path = conf['PATHS']['data_folder']
    

    @classmethod
    def config_from_file(cls, filename: str):
        cls.conf = ConfigParser().read(os.path.abspath(filename))._sections
