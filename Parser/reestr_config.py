
"""Конфигуратор из файла конфигурации config.ini"""
import os
from configparser import ConfigParser

class ReestrConfig:
    basedir = os.path.abspath(os.path.dirname((__file__)))
    config_path = os.path.join(basedir, 'config.ini')

    def __init__(self):

        self.config_proxy = None
        self.proxy_auth = None
        self.config_ssl = None

        if os.path.exists(self.config_path):
            config_file = ConfigParser()
            config_file.read(self.config_path)
        
            os.environ['DATA_FOLDER_PATH'] = os.path.abspath(config_file["DEFAULT"]["data_folder"])

            os.environ['LOGFILE'] = os.path.abspath(config_file["DEFAULT"]["logfile"])

            

            # Настройки для прокси через российский VDS
            if "PROXY" in config_file:
                proxy_host = config_file["PROXY"]["proxy_host"]
                proxy_port = config_file["PROXY"]["proxy_port"]
                proxy_user = config_file["PROXY"]["proxy_user"]
                proxy_pass = config_file["PROXY"]["proxy_pass"]

                self.config_proxy = {"https": f"socks5://{proxy_host}:{proxy_port}"}

                if proxy_user == 'None':
                    self.proxy_auth = (proxy_user, proxy_pass)
                
            # Настройка SSL
            if "SSL" in config_file:
                cert = os.path.relpath(config_file["SSL"]["key"], os.getcwd())
                self.config_ssl = cert