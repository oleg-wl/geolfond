#!venv/bin/python3
import traceback
from datetime import datetime
from src import panda

def run_code():

    df = panda.ReestrData()
    df.config()

    #Добавляет и сохраняет выгрузку

    with open(df.logfile, mode='a') as logs:
        try:
            logs.write(f'\nStart - {datetime.now()} \n')
            df.create_df()
            df.save()
            logs.write(f'{datetime.now()}, success. Saved in {df.path}')
        

        except:
            logs.write(f'ERROR - {datetime.now()}')
            traceback.print_exc(file=logs)

if __name__ == "__main__":
    run_code()