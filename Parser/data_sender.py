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

from exchangelib import Credentials, Account
from exchangelib import Message, Mailbox, FileAttachment, HTMLBody

from .reestr_config import create_config
from .reestr_config import check_path
from .reestr_config import logger as _logger
from .reestr_config import EmptyFolder
from .reestr_config import serv


class EmailSender:

    def __init__(self) -> None:

        self.logger = _logger()

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
        
        self.folder = check_path()
        self.files = glob.glob(rf'{self.folder}/*.xlsx')
        #: проверка что в папке есть файлы для отправки
        if len(self.files) == 0: 
            self.logger.debug(f'В папке {self.files} нет файлов')
            raise EmptyFolder()

        self.user = self.smtp_user or None
        
    #@basic_logging(msg='Создаю сообщенение', error='Ошибка при создании сообщения')
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
        #TODO доделать присоединение картинок к присьму 
        if image:
                        
            ab = os.path.join(self.folder, 'benzin.jpg')
            dt = os.path.join(self.folder, 'disel.jpg')
            
            with open(ab, 'rb') as img:
                i = base64.encode(img.read())
                i = base64.decode()
                #i = base64.b64decode(i)
            s = MIMEText(f'<img src="data:image/jpeg;base64"{i}>', 'html')
            msg.attach(s)
            #with open(dt, 'rb') as img:
            #    msgImage2 = MIMEImage(img.read())
            #    msgImage2.add_header('Content-ID', f'<img2>')
            
        #msg.attach(msgImage1)
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


    def owa_message(self, subj, msg, attch=None):

        if self.conf.has_section('OWA'):
            srv = serv
            usr = rf'{self.conf.get("OWA", "owa_user")}'
            psw = rf'{self.conf.get("OWA", "owa_password")}'
        else: 
            self.logger.error('Нет данных для подключения к Exchange серверу. Укажи в секции OWA в config.ini')
            raise NoSectionError()
        self.logger.debug(f'{srv}, {psw}, {usr}')
        #Данные для подключения к серверу exchangelib
        credentials = Credentials(srv, psw)
        account = Account(usr, credentials=credentials, autodiscover=True)

        rcp = [Mailbox(email_address=addr) for addr in self.smtp_to]
        
        m = Message(
            account=account,
            folder=account.sent,
            subject=subj,
            body=HTMLBody(msg),
            to_recipients=rcp
        )
        if (isinstance(attch, list)) and (len(attch) > 0):
            #: Отправить отдельные файлы из папки
            #: Добавить список эксель файлов во вложение к письму переданный filename
            self.files = [os.path.join(self.folder, file) for file in attch]

            c = 0
            for file in self.files:
                if not os.path.exists(file):
                    self.logger.info(f'Файл {file} отсутствует')
                    continue
    
                fn = file.rsplit(sep='/')[-1] #имя файлаа
                ft = file.rsplit(sep='.')[1]  #тип файла
                with open(file, 'rb') as f:
                    c += 1
            
                    m.attach(
                        FileAttachment(name=fn, content=f.read())
                    )

        m.send_and_save()