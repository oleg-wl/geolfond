from configparser import ConfigParser

from configparser import NoSectionError
import glob
import os
import base64
import logging

import smtplib, ssl

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from email import policy

from exchangelib import Credentials, Account
from exchangelib import Message, Mailbox, FileAttachment, HTMLBody

from ..base_config import BasicConfig

class EmailSender(BasicConfig):
        
    def __init__(self) -> None:

        self.logger = logging.getLogger('email-sender')
        self.message = None
        
        self.email_conf: dict = self.conf.get('email', False)
        self.owa: dict = self.conf.get('OWA', False)

        if self.conf:
            self.smtp_user: str = self.email_conf.get("smtp_user")
            self.smtp_pass: str = self.email_conf.get("smtp_password")
            self.smtp_server: str = self.email_conf.get("smtp_server")
            self.smtp_port: str = self.email_conf.get("smtp_port")
            self.smtp_to: list = self.email_conf.get("smtp_to").split(",")
            self.smt_to_2: str = self.email_conf.get('smtp_to_2')
        else:
            self.logger.error(
                "Необходимо указать данные для отправки email в config.ini"
            )
            raise NoSectionError('123')

        self.files = glob.glob(rf"{self.path}/*.xlsx")
        #: проверка что в папке есть файлы для отправки
        if len(self.files) == 0:
            self.logger.warning("В папке %s нет файлов. Нечего отправлять", self.files)

        self.user = self.smtp_user or None
        
    def create_message(
        self,
        subj: str, 
        htmlstr: str = None,
        filename: str | list = None,
        all: bool = False,
        image: bool = False,
    ) -> MIMEMultipart:
        """
        Создать MIME сообщение для отправки на почту. Если all=False, filename=None (default), отправить письмо без вложений.

        :param bool all: True - добавить все файлы из data_folder во вложение, defaults to False
        :param str | list filename: имя файла в папке data_folder или список имен файлов, defaults to None
        :param str htmlstr: html текст для добавления в тело письма, defaults to None
        :return _type_: MIMEMultipart
        """

        msg = MIMEMultipart(policy=policy.default)
        msg["Subject"] = subj
        msg["From"] = self.smtp_user
        msg["To"] = ", ".join(self.smtp_to)
        msg["Cc"]

        if (all) or (filename is not None):
            if (not all) and (isinstance(filename, str)):
                self.files = [os.path.join(self.path, filename)]

            if (not all) and (isinstance(filename, list)) and (len(filename) > 0):
                #: Отправить отдельные файлы из папки
                #: Добавить список эксель файлов во вложение к письму переданный filename
                self.files = [os.path.join(self.path, file) for file in filename]

            c = 0
            for file in self.files:
                if not os.path.exists(file):
                    self.logger.warning(f"Файл {file} отсутствует")
                    continue

                fn = file.rsplit(sep="/")[-1]  # имя файлаа
                ft = file.rsplit(sep=".")[1]  # тип файла
                with open(file, "rb") as f:
                    c += 1
                    attach = MIMEApplication(f.read(), _subtype=ft)

                attach.add_header("Content-Disposition", "attachment", filename=fn)
                msg.attach(attach)

            self.logger.info(f"{c} добавлено во вложения")

        if isinstance(htmlstr, str):
            msg.attach(MIMEText(htmlstr, "html"))

        # добавить jpg в сообщение. Добавит две картинки из папки дата в сообщение (1) бензиновые средние нарастающим итогом за месяц, (2) дизельные средние нарастающим итогом за
        # TODO доделать присоединение картинок к присьму
        if image:
            ab = os.path.join(self.path, "benzin.jpg")
            dt = os.path.join(self.path, "disel.jpg")

            with open(ab, "rb") as img:
                i = base64.encode(img.read())
                i = base64.decode()
                # i = base64.b64decode(i)
            s = MIMEText(f'<img src="data:image/jpeg;base64"{i}>', "html")
            msg.attach(s)
            # with open(dt, 'rb') as img:
            #    msgImage2 = MIMEImage(img.read())
            #    msgImage2.add_header('Content-ID', f'<img2>')

        # msg.attach(msgImage1)
        # msg.attach(msgImage2)

        # вовзаращет своейство класса сообщение
        self.message = msg
        return self

    def send_message_f(self) -> None:
        """
        Метод для отправки сообщения
        """
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(
            self.smtp_server, port=self.smtp_port, context=context
        ) as server:
            server.login(user=self.smtp_user, password=self.smtp_pass)
            server.send_message(msg=self.message)
        t = self.message.get("To")
        self.logger.info(f"Выгрузка отправлена на адрес {t}")

    def owa_message(self, subj, msg, attch=None):
        if self.owa:
            srv = self.owa.get("owa_server").replace('@','\\')
            usr = rf'{self.owa.get("owa_user")}'
            psw = rf'{self.owa.get("owa_password")}'
        else:
            self.logger.error(
                "Нет данных для подключения к Exchange серверу. Укажи в секции OWA в config.ini"
            )
            raise NoSectionError
        
        self.logger.debug(f"{srv}, {psw}, {usr}")
        
        # Данные для подключения к серверу exchangelib
        credentials = Credentials(srv, psw)
        account = Account(usr, credentials=credentials, autodiscover=True)

        rcp = [Mailbox(email_address=addr) for addr in self.smtp_to]

        m = Message(
            account=account,
            # folder=account.sent,
            subject=subj,
            body=HTMLBody(msg),
            to_recipients=rcp,
        )
        if (isinstance(attch, list)) and (len(attch) > 0):
            #: Отправить отдельные файлы из папки
            #: Добавить список эксель файлов во вложение к письму переданный filename
            self.files = [os.path.join(self.path, file) for file in attch]

            c = 0
            for file in self.files:
                if not os.path.exists(file):
                    self.logger.warning(f"Файл {file} отсутствует")
                    continue

                fn = file.rsplit(sep="/")[-1]  # имя файлаа
                ft = file.rsplit(sep=".")[1]  # тип файла
                with open(file, "rb") as f:
                    c += 1

                    m.attach(FileAttachment(name=fn, content=f.read()))

        m.send()
