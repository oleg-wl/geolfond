#!venv/bin/python

#--------------------------------------------------------------------------
# Скрипт для загрудки средних цен Цабвр и Цдтвр в текущем месяце
# Добавь в cron или systemd.timer для получения резултата на почту 
# Указанную в config.ini по расписанию
#--------------------------------------------------------------------------

import Parser
import datetime

def main():

    try: 
        n = datetime.datetime.strftime(datetime.datetime.now(), '%d.%m.%Y - %H:%M')
        
        c = Parser.client()
        data = c.get_abdt_index()
        m = Parser.transformer(data=data)
            
        s = m.kdemp()

        ms = Parser.sender()
        ms.create_message(filename=None, htmlstr=s)
        ms.message.replace_header('Subject', f'Текущие лимиты Кдемп - {n}')
        ms.send_message_f()
    except Exception as e:
        logs = Parser.reestr_config.config_logger()
        logs.exception(f'{e}')
        errmsg = Parser.sender()
        errmsg.create_log_message()
        errmsg.send_message_f()
        
if __name__ == "__main__":
    main()