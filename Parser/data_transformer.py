#!venv/bin/python3
import re
import os
import logging

import pandas as pd
import numpy as np


def check_path():
    return os.environ['DATA_FOLDER_PATH'] if os.environ.get('DATA_FOLDER_PATH') is not None else os.path.abspath('data')
 

def create_df(raw_data: list) -> pd.DataFrame: 
    """
    Очистка ответа API с помощью библиотеки pandas. Функция возвращает обработанный датафрейм.

    :param list raw_data: список словарей ответа api
    :return pd.DataFrame: очищенные данные
    """

    types = {
        "date": 'np.datetime64[D]',
        "Year": "int",
        'month': "int",
        "name": "str",
        "INN": "str",
        'owner': 'str',
        "N": "str",
        "E": "str",
        'rad_N':'float',
        "rad_E":'float',
        "prev_lic": "str",
        "prev_date": 'np.datetime64[D]',
        "forw_lic": "str",
        "forw_date": 'np.datetime64[D]',
        "type": "str",
        "state": "str",
        "Last": "bool",
        "filter": 'str'
    }

    df = pd.DataFrame(raw_data).set_index("num")
    
    # выделение столбца ГОД
    df["date"] = pd.to_datetime(
        df["date"], format="%Y-%m-%d", yearfirst=True
    )
    df["Year"] = df["date"].dt.year  # Год лицензии
    df["Year"] = df["Year"].astype('int')
    
    # Условный столбец последняя лицензия
    df["Last"] = np.where(
        df['forw_full'].isna(),
        True,
        False,
    )  

    # Извлечение ИНН:
    # Паттерны regrex
    pattern_for_inn = "([\d{10}|\d{12}]+)"
    pattern_for_replace_inn = "( \(ИНН.*\)$)"

    df["INN"] = df["owner_full"].str.extract(pattern_for_inn)
    df["INN"] = df["INN"].astype('int', errors="ignore")
    df["owner"] = df["owner_full"].str.replace(pattern_for_replace_inn, "", regex=True)

    #: Очистка для столбца с видом полезного ископаемого
    df['filter'] = df['filter'].str.slice(start=4) 

    #: Функция для очистки данных о недропользователях
    #: Функция изменяет ИНН на последний для дублкатов, когда несколько ИНН у одного наименования недропользовтаеля
    #: Как праило это ликвидированные или реорганизованные недропользователи    
    def change_inn(owners):
        for owner in owners:
            try:
                query = df.loc[(df['owner'] == owner),  ['INN','owner','Year']].sort_values(by='Year', ascending=False)
                df.loc[df['owner'] == owner, 'INN'] = query.iloc[0,0]
            except Exception:
                continue
    def change_owners(inns):
        for inn in inns:
            try:
                query = df.loc[(df['INN'] == inn),  ['INN','owner','Year']].sort_values(by='Year', ascending=False)
                df.loc[df['INN'] == inn, 'owner'] = query.iloc[0,1]
            except Exception:
                continue

    #: Оптимизация для фильтров не oil
    logging.debug(f'Очистка данных о недропользователях')
    
    if df['filter'].str.contains('Углеводородное сырье').any():
        change_inn(df['owner'].unique().tolist())
        change_owners(df['INN'].unique().tolist())
    logging.debug(f'Finished')

    df["owner"] = df["owner"].fillna(value=df["owner_full"], axis=0)
    
    forms = {'ЗАКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО':'ЗАО',
                'ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ':'ИП',
                'НЕПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО':'НПАО',
                'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ':'ООО',
                'ОТКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО':'ОАО',
                'ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО':'ПАО',
                'ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ УЧРЕЖДЕНИЕ':'ФГБУ',
                'ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ УНИТАРНОЕ ГЕОЛОГИЧЕСКОЕ ПРЕДПРИЯТИЕ':'ФГУ ГП',
                'АКЦИОНЕРНОЕ ОБЩЕСТВО':'АО',}

    df['owner'] = df['owner'].str.upper()
    for k, v in forms.items():
        df['owner'] = df['owner'].str.replace(pat=k, repl=v)

    # STEP PREV_FORW_LIC
    # Извлечение номеров предыдущих и будущих:
    pattern_for_lic = (
        "([А-Я]+[0-9]+[-А-Я]+)"  # Паттерн для извлечения номера ранее выданной лицензии
    )
    pattern_for_date = (
        "([\d\.]+$)"  # Паттерн для извлечения даты ранее выданной лицензии
    )

    # Извлечение данных предыдущих лицензий
    df["prev_lic"] = df["prew_full"].str.extract(pattern_for_lic)
    df["prev_date"] = df["prew_full"].str.extract(pattern_for_date)
    df["prev_date"] = pd.to_datetime(
        df["prev_date"], format="%d.%m.%Y", errors="ignore", dayfirst=True
    )

    # Извлечение данных будущих лицензий
    df["forw_lic"] = df['forw_full'].str.extract(pattern_for_lic)
    df["forw_date"] = df['forw_full'].str.extract(pattern_for_date)
    df["forw_date"] = pd.to_datetime(
        df["forw_date"], format="%d.%m.%Y", errors="ignore", dayfirst=True
    )

    # STEP COORDS
    # Извлечение координат:
    # Паттерн regrex
    pattern_coord = "(?P<N>\d*°\d*'[\d\.]+\"N)(?: *)(?P<E>[\d-]*°\d*'[\d\.]+\"[E|Е])"

    coords = (
        df['coords']
        .str.extractall(pattern_coord, re.MULTILINE)
        .sort_values(by="N", ascending=False)
        .droplevel(1)
    )
    coords = coords[coords.index.duplicated(keep="first") == False]

    # Объединение датафрейма
    df = pd.merge(df, coords, how="left", left_index=True, right_index=True)

    # Функция для извлечения радиан из столбцов N и E
    def to_rads(value) -> float:

        pattern = "(\d*)°(\d*)'(\d*\.*\d*)"
        val = re.findall(pattern=pattern, string=value)[0]
        return round(float(val[0]) + float(val[1])/60 + float(val[2])/3600, 5)

    # Добавление столбцов
    # Перевод координат град-мин-сек в радианы
    df.loc[:,'rad_N'] = df['N'].map(arg=to_rads, na_action='ignore')
    df.loc[:,'rad_E'] = df['E'].map(arg=to_rads, na_action='ignore')

    df['rad_N'] = pd.to_numeric(df['rad_N'], downcast='float', errors='coerce')
    df['rad_E'] = pd.to_numeric(df['rad_E'], downcast='float', errors='coerce')
    #Добавление периодов
    df.loc[:,'month'] = df['date'].dt.to_period('M')


    # STEP PREV_OWNER
    # Добавление столбца предыдущего владельца лицензии
    df = df.loc[~df.index.duplicated(keep="last")]  # Убираем дубликаты из индекса

    prev_owner_df = df.reset_index().set_index("forw_lic")
    prev_owner_df = prev_owner_df[prev_owner_df.index.notnull()]
    prev_owner_df = prev_owner_df[~prev_owner_df.index.duplicated(keep="last")][
        "owner"
    ].rename("prev_owner")

    # Присоединение столбца с данными о предыдущих владельцах
    df = pd.concat([df, prev_owner_df], axis=1, join="outer").rename_axis(
        "num"
    )
    df = df[df["date"].notna()]

    df = df[list(types.keys())].reset_index()

    logging.info(f'Данные обработаны {len(df.index)}')

    #: Тесты
    try: 
        assert df['prew_full'].count() == df['prev_lic'].count(), f"Тест не пройден, prew_lic:{df['prev_lic'].count()}, prew_full:{df['prew_full'].count()}"

        assert df['forw_full'].count() == df['forw_lic'].count(), f"Тест не пройден, forw_lic: {df['forw_lic'].count()}, forw_full: {df['forw_full'].count()}"

        assert (df['N'].count() == df['coords'].count()) & (df['E'].count() == df['coords'].count()), f"Тест не пройден, N: {df['N'].count()}, E: {df['E'].count()}, coords: {df['coords'].count()}"

        assert df['owner'].count() == df['owner_full'].count(), f"Тест не пройден, owner: {df['owner'].count()}, owner full: {df['owner_full'].count()}"
        logging.info('Tests OK!')

    except AssertionError as e:
        logging.critical(e)
    
    return df.astype(dtype=types)

def save_df(dataframe: list | pd.DataFrame, name: str) -> None:
    """
    Функция для сохранения результата обработки в ексель файл

    :param list  |  pd.DataFrame dataframe: датафрейм для сохранения в ексель
    :param str name: фильтр для названия файла
    """

    excel_path = os.path.join(check_path(), f'{name}_data.xlsx')
    dataframe.to_excel(excel_path, sheet_name=name, freeze_panes=(1, 0), na_rep='')

    logging.info(f'Данные сохранены в {excel_path}')
    
def create_matrix(dataframe: pd.DataFrame) -> None:
    """
    Функция создает матрицу для меппенига для ГБЗ
    :param pd.DataFrame dataframe: Пандас Датайфрейм с очищенными данными
    """
    #Очистить данные о предыдущих лицензиях
    dataframe['prew_'] = dataframe['prew_full'].str.replace('\n', ':', regex=True).str.replace(' от \d\d\.\d\d', '', regex=True)

    #Извлечь все предыдущие лицензии и их год
    pattern = "(?P<old_lic>[А-Я]{3}\d*[А-Я]{2})(?:.)(?P<old_year>\d{4})"
    df_left = dataframe['prew_'].str.extractall(pattern).droplevel(1).astype(dtype={'old_year':'int'})

    #Добавить все текущие лицензии и их год
    df_right = dataframe.loc[dataframe['Last'] == True, ['Year']].astype('int')

    #Таблица для связи текущих лицензий с предыдущими
    lookup_table = df_left.merge(df_right, how='right', left_index=True, right_index=True)
    lookup_table = lookup_table[['Year', 'old_lic', 'old_year']]    #.to_excel(excel_writer='data/test.xlsx')

    #Создание датафрейма для матрицы соотношения номеров старых лицензий с текущими лицензиями
    index = list(set(lookup_table.index.to_list()))
    columns = list(set(lookup_table.Year.tolist()))

    matrix = pd.DataFrame(data = None, index = index, columns=columns)

    #Алгоритм заполнения датафрейма
    for i in lookup_table.itertuples():
        ind = i[0]
        col = i[1]
        old_lic = i[2]
        old_year = i[3]
        matrix.loc[ind, col] = ind
        matrix.loc[ind, old_year] = old_lic

    #Заполнение пустых ячеек вправо
    matrix = matrix.ffill(axis=1)
    
    matrix_path = os.path.join(check_path(), '_matrix.xlsx')
    if os.path.exists(matrix_path):
        with pd.ExcelWriter(path=matrix_path, if_sheet_exists='replace', mode='a') as writer:
            matrix.to_excel(writer, sheet_name='matrix')
            lookup_table.to_excel(writer, sheet_name='lookup_table')
    else: 
        matrix.to_excel(matrix_path, sheet_name='matrix')
        with pd.ExcelWriter(path=matrix_path, if_sheet_exists='replace', mode='a') as writer:
            lookup_table.to_excel(writer, sheet_name='lookup_table')
