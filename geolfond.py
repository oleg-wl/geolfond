#!venv/bin/python3
import traceback
from datetime import datetime
from Parser import parser

def run_code():

    #Добавляет и сохраняет выгрузку
    with open(parser.logfile, mode='a') as logs:
        try:
            logs.write(f'\nStart - {datetime.now()} \n')
            parser.create_df()
            logs.write(f'{datetime.now()}, success. Saved in {parser.path}')
        

        except:
            logs.write(f'ERROR - {datetime.now()}')
            traceback.print_exc(file=logs)

if __name__ == "__main__":
    run_code()