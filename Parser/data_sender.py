import glob

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

        folder = check_path()
        self.files = glob.glob(rf'{folder}/*.xlsx')

    def create_message(self) -> EmailMessage:

        msg = EmailMessage()
        msg['Subject'] = 'Выгрузка данных для дашборда'
        msg['From'] = self.conf.smtp_email
        msg['To'] = self.conf.smtp_to

        if len(self.files) > 0: 
            for file in self.files:
                
                with open(file, 'rb') as f:
                    attach = f.read()

                filename = file.rsplit(sep='/')[-1]
                 
                msg.add_attachment(attach, maintype='application', subtype='xlsx', filename=filename)
            self.logger.info(f'{len(self.files)} добавлено во вложения')
            
        else: 
            self.logger.debug(f'В папке {self.files} нет файлов')
            raise EmptyFolder(f'В папке {self.files} нет файлов')

        with open(self.logfile, 'rb') as log:
            attach = log.read()
        msg.add_attachment(attach, maintype='application', subtype='txt', filename=self.logfile) 
        msg.add_header('Content-Disposition', 'attachment')

        return msg

    def send_message(self, msg) -> None:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL(self.conf.smtp_server, port=self.conf.smtp_port, context=context) as server:
            server.login(user=self.conf.smtp_email, password=self.conf.smtp_password)
            server.send_message(msg)
        self.logger.info(f'Выгрузка отправлено на адрес {self.conf.smtp_to}')