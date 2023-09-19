from configparser import NoSectionError
import glob
import os

import smtplib, ssl
from email.message import EmailMessage

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

from .reestr_config import create_config
from .reestr_config import check_logfile, check_path, config_logger
from .reestr_config import EmptyFolder

class EmailSender:

    def __init__(self) -> None:

        self.logger = config_logger('email-sender')

        self.message = None

        self.conf = create_config()
        if self.conf.has_section('email'):
            self.smtp_user = self.conf.get('email', 'smtp_user')
            self.smtp_pass = self.conf.get('email', 'smtp_password')
            self.smtp_server = self.conf.get('email', 'smtp_server')
            self.smtp_port = self.conf.get('email', 'smtp_port')
            self.smtp_to = self.conf.get('email', 'smtp_to').split(',')
        else: 
            self.logger.error('Необходимо указать данные для отправки email в config.ini') 
            raise  NoSectionError()
        
        self.logfile = check_logfile()
        self.folder = check_path()
        self.files = glob.glob(rf'{self.folder}/*.xlsx')
        #: проверка что в папке есть файлы для отправки
        if len(self.files) == 0: 
            self.logger.debug(f'В папке {self.files} нет файлов')
            raise EmptyFolder()

        self.user = self.smtp_user or None
        

    def create_message(self, all: bool = False, filename: str = None, htmlstr: str = None):
        """
        Создать сообщение с вложением для отправки на почту

        :param bool all: True для отправки всех файлов из папки data в config.ini, defaults to False
        :param str filename: Имя файла для отправки. Если all=True будет ошибка, defaults to None
        """

        msg = MIMEMultipart()
        msg['Subject'] = 'Выгрузка данных для дашборда'
        msg['From'] = self.smtp_user
        msg['To'] = ", ".join(self.smtp_to)

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
                if not os.path.exists(f):
                    self.logger.debug(f'Путь к файлу для отправки не существует: {f}')
                    raise FileNotFoundError('Файл несуществует')
                else:
                    with open(f, 'rb') as file:
                        attach = MIMEApplication(file.read(), _subtype='xlsx')
                    attach.add_header('Content-Disposition', 'attachment', filename=filename)
                    msg.attach(attach)                    
                    self.logger.info(f'{filename} добавлен во вложениe')

        #: Отпрввить все xlsx в папке
        elif all:
        #: Добавить все эксель файлы во вложение к письму
            for file in self.files:
                with open(file, 'rb') as f:
                    attach = MIMEApplication(f.read(), _subtype='xlsx')
                   
                filename = file.rsplit(sep='/')[-1]
                attach.add_header('Content-Disposition', 'attachment', filename=filename)
                msg.attach(attach)
                    
            self.logger.info(f'{len(self.files)} добавлено во вложения')
        
        if isinstance(htmlstr, str):
            msg.attach(MIMEText(htmlstr, 'html'))    
         


        self.message = msg
        return self

    def send_message_f(self) -> None:
        """
        Метод для отправки сообщения
        """
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(self.smtp_server, port=self.smtp_port, context=context) as server:
            server.login(user=self.smtp_user, password=self.smtp_pass)
            server.send_message(msg=self.message)
        t = self.message.get('To')
        self.logger.info(f'Выгрузка отправлено на адрес {t}')

    def create_log_message(self) -> EmailMessage:


        msg = EmailMessage()
        msg['Subject'] = 'Ошибка выгрузки. Лог файл'
        msg['From'] = self.smtp_user

        #send log to first if multiple addrs 
        if isinstance(self.smtp_to, list): 
            msg['To'] = self.smtp_to[0] 
        elif isinstance(self.smtp_to, str): msg['To'] = self.smtp_to 

        #: msg
        with open(self.logfile, 'rb') as log:
            attach = log.read()
        msg.add_attachment(attach, maintype='application', subtype='txt', filename=self.logfile) 
        msg.add_header('Content-Disposition', 'attachment')
        return msg
    