#!venv/bin/python

# --------------------------------------------------------------------------
# Скрипт для загрудки средних цен для отслеживания индикатива Цабвр и Цдтвр в текущем месяце
# Добавь в cron или systemd.timer для получения резултата на почту
# Указанную в config.ini по расписанию
# --------------------------------------------------------------------------

import Parser
import datetime


def main():
    logger = Parser._logger()
    try:
        n = datetime.datetime.strftime(datetime.datetime.now(), "%d.%m.%Y - %H:%M")

        c = Parser.client()
        data = c.get_abdt_index()
        m = Parser.transformer(data=data)

        s = m.kdemp()

        ms = Parser.sender()

        ms.owa_message(
            subj=f"Текущие лимиты Кдемп - {n}",
            msg=s,
            attch=["СредняяЦенаАБДТ.xlsx", "СредняяЦенаАБДТ_накоп.xlsx"]
        )
    except Exception as e:
        logger.exception(e)


if __name__ == "__main__":
    main()
