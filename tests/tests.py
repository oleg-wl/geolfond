#!venv/bin/python3
# -*- coding=utf-8 -*-

import sys
sys.path.append('.')
from src.panda import ReestrData

dataset = ReestrData()
dataset.config()
test = dataset.create_df()


print (test['prev_lic'].count(), test['prew_full'].count())
print (test['forw_lic'].count(), test['forw_full'].count())
print(test['N'].count(), test['E'].count(), test['coords'].count())
print(test['owner'].count(), test['owner_full'].count())

try: 
    assert test['prew_full'].count() == test['prev_lic'].count(), 'Ошибка в выгрузке предыдущих лицензий'
    assert test['forw_full'].count() == test['forw_lic'].count(), 'Ошибка в выгрузке будущих лицензий'
    assert (test['N'].count() == test['coords'].count()) & (test['E'].count() == test['coords'].count()), 'Coords_extract_error'
    assert test['owner'].count() == test['owner_full'].count(), 'Name_extract_error'

except AssertionError as e:
    print(e)
