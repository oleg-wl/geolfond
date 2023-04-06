"""
Скрипт для сравнения двух датафреймов из папки с выгруженными файлами. 
1. Получение листа с названиями файлов
2. выбор первого и последнего файла

Сделай отдельную функцию для сохранения и отдельную для получения датафрейма
"""

import os
import pathlib
import re
import pandas as pd
import datetime

def file_list(dir_path):
    """
    Функция принимает путь к папке после user и возвращает список файлов по паттерну 
    tilda/dir1/dir2...
    """
    print('Получаю список файлов для сравнения')
    
    global file_pattern, dir_pattern
    dir_pattern = "*.xlsx"
    file_pattern = "[^\\\]+\.xlsx$"
    
    #Cоздание универсального пути к файлам:
    dir = pathlib.Path(os.path.normpath(os.path.expanduser(dir_path))) #Полный путь с учетом юзера в dir_path
    file_list = []
    for file in dir.glob(dir_pattern):
        list.append(file_list, str(file))
    
    for i,v in (enumerate(file_list)):
        print(i,re.findall(file_pattern, v))
    return file_list
 

def new_licenses(file_list, dir_path):
    '''
    Функция принимает лист c именами и путь для сохранения  файлов эксель (загрузи из функции file list). Преобразует эксель файлы в pandas DataFrame для объединения 
    '''
    now =  datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    dir_1 = file_list[int(input('Какой файл взять за основу:'))]
    dir_2 = file_list[int(input('С какмим файлом сравнить:'))]
    print('Работаю...')

    df1 = pd.read_excel(os.path.abspath(dir_1), index_col=0, header=0)
    df2 = pd.read_excel(os.path.abspath(dir_2), index_col=0, header=0)
    
    #Новый датафрейм 
    result_df = pd.merge(df1, df2, how='outer', left_index=True, right_index=True, suffixes=('_old', '_new'))
    result_df.to_excel(f'{os.path.abspath(os.path.expanduser(dir_path))}\compare_result_{now}.xlsx', sheet_name='CompairedData')
    
    # result_df = result_df.where(result_df['Сведения о пользователе недр_old'] != result_df['Сведения о пользователе недр_new']).drop()
    print (f'Я сравнил {re.findall(file_pattern, dir_1)} и {re.findall(file_pattern,dir_2)}. Результат сохранил в {dir_path}')
    return result_df


if __name__ == "__main__":
    
    dir_path = '~\Dropbox\Проекты\Роснедра\parser\Реестр' # Путь независимо от юзера
    df = new_licenses(file_list(dir_path), dir_path)
    
    print(df.head())




