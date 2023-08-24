#!venv/bin/python3
import logging
from Parser import client
import Parser

logging.basicConfig(filemode='w', filename='log/logs.log', format='%(levelname)s, %(asctime)s: %(message)s (LINE: (%(lineno)d) [%(filename)s]', datefmt='%I:%M:%S %p', level=logging.INFO)

def run_code():
    try:
        f = Parser.filter()
        for i in f.keys():
            data = client.get_data_from_reestr(filter=i)
            logging.info(f'загрузил {i}, записей {len(data)}') 
            df = Parser.create_df(data)
            logging.info(f'конвертнул {i}')
            Parser.save_df(df, i)
            logging.info(f'сохранил {i}')
        logging.info('Success!')

    except Exception as e:
        logging.critical(f"{e}")    


if __name__ == "__main__":
    run_code()