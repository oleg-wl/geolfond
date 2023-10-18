#!venv/bin/python

#--------------------------------------------------------------------------
# Скрипт для загрудки средних цен Цабвр и Цдтвр в текущем месяце
# Добавь в cron или systemd.timer для получения резултата на почту 
# Указанную в config.ini по расписанию
#--------------------------------------------------------------------------

import Parser
import datetime

def main():

    logger = Parser._logger()
    try: 
        n = datetime.datetime.strftime(datetime.datetime.now(), '%d.%m.%Y - %H:%M')
        
        c = Parser.client()
        data = c.get_abdt_index()
        m = Parser.transformer(data=data)
            
        s = m.kdemp()

        ms = Parser.sender()
        ms.create_message(filename=['СредняяЦенаАБДТ_накоп.xlsx','СредняяЦенаАБДТ.xlsx'], htmlstr=s, image=False)
        ms.message.replace_header('Subject', f'Текущие лимиты Кдемп - {n}')
        ms.send_message_f()
    except Exception as e:
        logger.exception(e)
        
if __name__ == "__main__":
    main()