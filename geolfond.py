#!venv/bin/python3
import logging
from Parser import client, data

logging.basicConfig(filemode='w', filename='log/logs.log', format='%(levelname)s, %(asctime)s: %(message)s (LINE: (%(lineno)d) [%(filename)s]', datefmt='%I:%M:%S %p', level=logging.INFO)

def run_code():
    try:
        client.get_data_from_reestr(filter='oil')[0]
        #data.create_df(client.get_data_from_reestr(filter='oil'))
        #data.create_matrix()
        logging.info('Success!')
    except Exception as e:
        logging.critical(f"{e}")    


if __name__ == "__main__":
    run_code()