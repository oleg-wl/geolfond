from zipfile import ZipFile
import json

def unpack():
    with ZipFile(file='data/oil.zip', mode='r') as zip:
        with zip.open('oil', mode='r') as data:
            return json.loads(data.read())