from fastapi import FastAPI
from unpack import unpack

app = FastAPI()

data = unpack()

@app.get('/')
async def root():
    pass

@app.get('random')
async def random_row():
    #todo: return random row
    pass

"""

"""