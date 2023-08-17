#!venv/bin/python3
import traceback
from datetime import datetime
from Parser import client, data

def run_code():

    #Добавляет и сохраняет выгрузку
    with open(client.logfile, mode='a') as logs:
        try:
            logs.write(f'\nStart - {datetime.now()} \n')
            
            data.create_df(client.get_data_from_reestr(filter='oil'))
            data.save()
            #data.create_matrix()

            logs.write(f'{datetime.now()}, success. Saved in data\n')
        

        except:
            logs.write(f'ERROR - {datetime.now()}\n')
            traceback.print_exc(file=logs)

if __name__ == "__main__":
    run_code()
