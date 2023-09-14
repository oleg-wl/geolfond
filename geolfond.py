#!venv/bin/python3

"""
Основной модуль для запуска парсера. 

"""


import Parser


#logging.basicConfig(format='%(levelname)s, %(asctime)s: %(message)s (LINE: (%(lineno)d) [%(filename)s]', datefmt='%I:%M:%S %p', level=logging.DEBUG)

def parse_reestr_full() -> None:
    #: Пропарсить весь реестр
    try:
        logger = Parser.config_logger('geolfond_all_filters')
        parser = Parser.client()
        tr = Parser.transformer()
        f = Parser._filter()
        for i in f.keys():
            logger.info(f'Загружаю {i}')
            data = parser.get_data_from_reestr(filter=i)
            logger.info(f'Загрузил {i}')
            df = tr.create_df(data)
            logger.info(f'Сохранил в {tr.path}')
            tr.save_df(df, i)

    except Exception as e:
        logger.error(e)
        raise e

def run_code() -> None:
    #: Функция для скачивания только oil

    try:
        logger = Parser.config_logger('geolfond')
        parser = Parser.client()
        transformer = Parser.transformer()
        logger.info('Начинаю загрузку')

        data = parser.get_data_from_reestr(filter='oil')
        curr = parser.get_currency('01.01.2021')
        pr = parser.get_oil_price(rng=7)

        df = transformer.create_df(data)
        transformer.save_df(df, 'oil')

        transformer.create_prices(curr=curr, pr=pr)

        x = Parser.sender()
        msg = x.create_message(all=True)
        x.send_message(msg)
    except: 
        logger.critical('Ошибка выгрузки')
        x = Parser.sender()
        msg = x.create_log_message()
        x.send_message(msg)

        raise

if __name__ == "__main__":
    run_code()