import glob
import os

import smtplib, ssl
from email.message import EmailMessage

from .reestr_config import ReestrConfig
from .reestr_config import check_logfile, check_path, config_logger
from .reestr_config import EmptyFolder

class EmailSender:

    def __init__(self) -> None:

        self.logger = config_logger('email-sender')
        self.conf = ReestrConfig()
        self.logfile = check_logfile()

        self.folder = check_path()
        self.files = glob.glob(rf'{self.folder}/*.xlsx')
        #: проверка что в папке есть файлы для отправки
        if len(self.files) == 0: 
            self.logger.debug(f'В папке {self.files} нет файлов')
            raise EmptyFolder(f'В папке {self.files} нет файлов')

        self.user = self.conf.smtp_email or None
        

    def create_message(self, all: bool = False, filename: str = None) -> EmailMessage:
        """
        Создать сообщение с вложением для отправки на почту

        :param bool all: True для отправки всех файлов из папки data в config.ini, defaults to False
        :param str filename: Имя файла для отправки. Если all=True будет ошибка, defaults to None
        :return EmailMessage: инстанс класса EmailMessage для отправки
        """

        msg = EmailMessage()
        msg['Subject'] = 'Выгрузка данных для дашборда'
        msg['From'] = self.conf.smtp_email
        msg['To'] = self.conf.smtp_to

        #: проверка типов
        if (not all) and (filename is None):
            raise ValueError(f'Укажи имя файла для отправки или all = True для отправки всех файлов из папки {self.folder}')
        elif (not all):
            if not isinstance(filename, str):
                raise TypeError(f'{filename} должен быть строкой')
            elif (isinstance(filename, str)) and (not filename.__contains__('.xlsx')):
                raise ValueError('Укажи .xlsx файл')

            #: Отправить файл
            else:
                f = os.path.join(self.folder, filename)
                with open(f, 'rb') as file:
                    attach = file.read()
                    
                msg.add_attachment(attach, maintype='application', subtype='xlsx', filename=filename)
                self.logger.info(f'{filename} добавлен во вложениe')

        #: Отпрввить все xlsx в папке
        elif all:
        #: Добавить все эксель файлы во вложение к письму
            for file in self.files:
                with open(file, 'rb') as f:
                    attach = f.read()
                filename = file.rsplit(sep='/')[-1]
                   
                msg.add_attachment(attach, maintype='application', subtype='xlsx', filename=filename)
            self.logger.info(f'{len(self.files)} добавлено во вложения')
            
         

        with open(self.logfile, 'rb') as log:
            attach = log.read()
        msg.add_attachment(attach, maintype='application', subtype='txt', filename=self.logfile) 
        msg.add_header('Content-Disposition', 'attachment')

        return msg

    def send_message(self, msg: EmailMessage) -> None:
        """
        Метод для отправки сообщения

        :param EmailMessage msg: сообщение из метода create_message
        """
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(self.conf.smtp_server, port=self.conf.smtp_port, context=context) as server:
            server.login(user=self.conf.smtp_email, password=self.conf.smtp_password)
            server.send_message(msg)
        self.logger.info(f'Выгрузка отправлено на адрес {self.conf.smtp_to}')

    def error_log_message():
        #: Отделный метод для отправки сообщения в случае ошибки
        #TODO сделать как декоратор
        pass