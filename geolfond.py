#!venv/bin/python3

#--------------------------------------------------------------------------
# Основной скрипт для загрудки информации из росгеолфонда, Ц и Р в НДПИ
# Добавь в cron или systemd.timer для получения резултата на почту 
# Указанную в config.ini по расписанию. Если адресатов несколько, результат
# получит последний адресат из списка
#--------------------------------------------------------------------------


import Parser

def parse_reestr_full() -> None:
    #: Пропарсить весь реестр
    #! В разработке
    try:
        logger = Parser.config_logger('geolfond_all_filters')
        parser = Parser.client()
        f = Parser._filter()
        for i in f.keys():
            logger.info(f'Загружаю {i}')
            data = parser.get_data_from_reestr(filter=i)
            logger.info(f'Загрузил {i}')
            tr = Parser.transformer(data=data)
            tr.create_df()
            logger.info(f'Обработал {i}, всего {len(tr.rosnedra)}')
            
            logger.info(f'Сохранил в')
            tr.save_df()

    except Exception as e:
        logger.error(e)
        raise e

def run_code() -> None:
    #: Функция для скачивания только oil

    try:
        logger = Parser.config_logger('geolfond')
        parser = Parser.client()
        logger.info('Начинаю загрузку')

        data = parser.get_data_from_reestr(filter='oil')
        logger.info('Данные загружены')
        
        tr = Parser.transformer(data=data)
        tr.create_df()
        tr.save_df(dataframe=tr.rosnedra, name='oil')

        curr = parser.get_currency('01.01.2021')
        pr = parser.get_oil_price(rng=7)
        tr.create_prices(curr=curr, pr=pr)

        ms = Parser.sender()
        print(ms.smtp_to)
        
        #Если в config.ini несколько адресатов, отправлять только на последний
        ms.smtp_to = [ms.smtp_to[-1]]
        print(ms.smtp_to)
        
        ms.create_message(all=False, filename=['prices.xlsx', 'oil.xlsx'], htmlstr='Выгрузка данных для дашборда')
        ms.send_message_f()
    except: 
        logger.critical('Ошибка выгрузки')
        errmsg = Parser.sender()
        errmsg.create_log_message()
        errmsg.send_message_f()

        raise

if __name__ == "__main__":
    run_code()