from configparser import NoSectionError
import glob
import os
import base64

import smtplib, ssl
from email.message import EmailMessage

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email import policy

from .reestr_config import create_config
from .reestr_config import check_logfile, check_path, config_logger, basic_logging
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
        
    @basic_logging(msg='Создаю сообщенение', error='Ошибка при создании сообщения')
    def create_message(self, all: bool = False, filename: str|list = None, htmlstr: str = None, image: bool = False):
        """
        Создать MIME сообщение для отправки на почту. Если all=False, filename=None (default), отправить письмо без вложений. 

        :param bool all: True - добавить все файлы из data_folder во вложение, defaults to False
        :param str | list filename: имя файла в папке data_folder или список имен файлов, defaults to None
        :param str htmlstr: html текст для добавления в тело письма, defaults to None
        :return _type_: MIMEMultipart
        """

        msg = MIMEMultipart(policy=policy.default)
        msg['Subject'] = 'Выгрузка данных для дашборда'
        msg['From'] = self.smtp_user
        msg['To'] = ", ".join(self.smtp_to)


        if (all) or (filename is not None):
        
            if (not all) and (isinstance(filename, str)):
                self.files = [os.path.join(self.folder, filename)]

            if (not all) and (isinstance(filename, list)) and (len(filename) > 0):
            #: Отправить отдельные файлы из папки
                #: Добавить список эксель файлов во вложение к письму переданный filename
                self.files = [os.path.join(self.folder, file) for file in filename]

            c = 0
            for file in self.files:
                if not os.path.exists(file):
                    self.logger.info(f'Файл {file} отсутствует')
                    continue
                
                fn = file.rsplit(sep='/')[-1] #имя файлаа
                ft = file.rsplit(sep='.')[1]  #тип файла
                with open(file, 'rb') as f:
                    c += 1
                    attach = MIMEApplication(f.read(), _subtype=ft)

                attach.add_header('Content-Disposition', 'attachment', filename=fn)
                msg.attach(attach)

            self.logger.info(f'{c} добавлено во вложения')

        if isinstance(htmlstr, str):
            msg.attach(MIMEText(htmlstr, 'html'))
            
        # добавить jpg в сообщение. Добавит две картинки из папки дата в сообщение (1) бензиновые средние нарастающим итогом за месяц, (2) дизельные средние нарастающим итогом за 
        if image:
            
            imgtxt = MIMEText('<br><img src="cid:img1"><img src="cid:img2">', 'html')
            msg.attach(imgtxt)
            
            ab = os.path.join(self.folder, 'benzin.jpg')
            dt = os.path.join(self.folder, 'disel.jpg')
            
            with open(ab, 'rb') as img:
                msgImage1 = MIMEImage(img.read())
                msgImage1.add_header('Content-ID', '<img1>')
            #with open(dt, 'rb') as img:
            #    msgImage2 = MIMEImage(img.read())
            #    msgImage2.add_header('Content-ID', f'<img2>')
            
        msg.attach(msgImage1)
        #msg.attach(msgImage2)
        
            


        #вовзаращет своейство класса сообщение
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
            msg['To'] = self.smtp_to[-1] 
        elif isinstance(self.smtp_to, str): msg['To'] = self.smtp_to 

        #: msg
        with open(self.logfile, 'rb') as log:
            attach = log.read()
        msg.add_attachment(attach, maintype='application', subtype='txt', filename=self.logfile) 
        msg.add_header('Content-Disposition', 'attachment')
        self.logger.info('Создан файл с логом')

        self.message = msg
        return self
    