#!venv/bin/python3
import traceback
from datetime import datetime
from src.panda import ReestrData

def run_code():

    y = ReestrData()
    y.config()

    with open(y.logfile, mode='a') as logs:
        try:
            logs.write(f'Start - {datetime.now()} \n')
            y.save()
            logs.write(f'{datetime.now()}, success. Saved in {y.path}')
        

        except:
            logs.write(f'ERROR - {datetime.now()}')
            traceback.print_exc(file=logs)

if __name__ == "__main__":
    run_code()