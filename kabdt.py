#!/usr/bin/env python
# -*- coding: utf-8 -*-

# --------------------------------------------------------------------------
# Скрипт для загрудки средних цен для отслеживания индикатива Цабвр и Цдтвр в текущем месяце
# Добавь в cron или systemd.timer для получения резултата на почту
# Указанную в config.ini по расписанию
# --------------------------------------------------------------------------

import Parser
import datetime
from email.mime.multipart import MIMEMultipart


def main():
    logger = Parser._logger()
    try:
        n = datetime.datetime.strftime(datetime.datetime.now(), "%d.%m.%Y - %H:%M")

        c = Parser.client()
        data = c.get_abdt_index()

        m = Parser.kdemp(data=data)

        s = m.kdemp()
        s = m.soup_html(s)

        ms = Parser.sender()

        ms.owa_message(
            subj=f"Текущие лимиты Кдемп - {n}",
            msg=s,
            attch=["СредняяЦенаАБДТ.xlsx", "СредняяЦенаАБДТ_накоп.xlsx"]
        )
        logger.info(f'Отправлено первое сообщение в адрес {ms.smtp_to}')
        
        
        ms.create_message(
            htmlstr=s,
            filename=["СредняяЦенаАБДТ.xlsx"])
        ms.message.replace_header('To', ms.smt_to_2)
        ms.message.replace_header('Subject', f"Текущие лимиты Кдемп - {n}")
        
        ms.send_message_f()
        logger.info(f'Отправлено второе сообщение в адрес {ms.message["To"]}')
        
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    main()
