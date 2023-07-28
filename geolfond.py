#!venv/bin/python3
import traceback
from datetime import datetime
from src.panda import ReestrMatrix

def run_code():

    y = ReestrMatrix()
    y.config()

    with open(y.logfile, mode='a') as logs:
        try:
            logs.write(f'\nStart - {datetime.now()} \n')
            y.save()
            y.create_matrix()
            logs.write(f'{datetime.now()}, success. Saved in {y.path}')
        

        except:
            logs.write(f'ERROR - {datetime.now()}')
            traceback.print_exc(file=logs)

if __name__ == "__main__":
    run_code()