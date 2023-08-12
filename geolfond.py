#!venv/bin/python3
import traceback
from datetime import datetime
from Parser import client, data
def run_code():

    #Добавляет и сохраняет выгрузку
    with open(client.logfile, mode='a') as logs:
        try:
            logs.write(f'\nStart - {datetime.now()} \n')

            df = client.get_data_from_reestr(filter='oil')[0]
            
            #data.create_df(client.get_data_from_reestr(filter='oil'))
            #data.create_matrix()

            logs.write(f'{datetime.now()}, success. Saved in {data.path}\n')
        

        except:
            logs.write(f'ERROR - {datetime.now()}\n')
            traceback.print_exc(file=logs)

if __name__ == "__main__":
    run_code()