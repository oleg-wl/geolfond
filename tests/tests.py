#!venv/bin/python3
# -*- coding=utf-8 -*-

import sys
sys.path.append('.')

from Parser import client, data

data.create_df(client.get_data_from_reestr(filter='oil'))


print (data.df['prev_lic'].count(), data.df['prew_full'].count())
print (data.df['forw_lic'].count(), data.df['forw_full'].count())
print(data.df['N'].count(), data.df['E'].count(), data.df['coords'].count())
print(data.df['owner'].count(), data.df['owner_full'].count())

try: 
    assert data.df['prew_full'].count() == data.df['prev_lic'].count(), 'Ошибка в выгрузке предыдущих лицензий'
    assert data.df['forw_full'].count() == data.df['forw_lic'].count(), 'Ошибка в выгрузке будущих лицензий'
    assert (data.df['N'].count() == data.df['coords'].count()) & (data.df['E'].count() == data.df['coords'].count()), 'Coords_extract_error'
    assert data.df['owner'].count() == data.df['owner_full'].count(), 'Name_extract_error'
    print ('Tests OK!')

except AssertionError as e:
    print(e)


#assert dataset['prew_'].count() == dataset['prew_full'].count(), 'Incorrect prew extract'
#assert len(lookup_table.index.unique()) == len(dataset[dataset['Last'] == True].index), "Error"