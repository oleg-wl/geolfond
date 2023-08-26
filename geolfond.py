#!venv/bin/python3
import logging
import Parser


logging.basicConfig(format='%(levelname)s, %(asctime)s: %(message)s (LINE: (%(lineno)d) [%(filename)s]', datefmt='%I:%M:%S %p', level=logging.DEBUG)

def parse_reestr_full() -> None:
    #: Пропарсить весь реестр
    try:
        f = Parser._filter()
        for i in f.keys():
            data = Parser.client.get_data_from_reestr(filter=i)
            logging.info(f'загрузил {i}, записей {len(data)}') 
            df = Parser.create_df(data)
            logging.info(f'конвертнул {i}')
            Parser.save_df(df, i)
            logging.info(f'сохранил {i}')
        logging.info('Success!')

    except Exception as e:
        logging.critical(f"{e}")    

def run_code() -> None:
    #: Функция для скачивания только oil

    data = Parser.client().get_data_from_reestr(filter='oil')
    logging.info(f'загрузил oil, записей {len(data)}') 

    df = Parser.create_df(data)
    logging.info(f'конвертнул')

    Parser.save_df(df, 'oil')
    logging.info(f'сохранил')

if __name__ == "__main__":
    run_code()