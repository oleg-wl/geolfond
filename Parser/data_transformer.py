#!venv/bin/python3
import re
import os

import pandas as pd
import numpy as np
import json

from .headers import cols as _cols

#TODO: Сделай здесь отдельный модуль для обработки и матрицы и в основном модуле parser.py запиши логику этих модулей отдельно
# как опция перенеси туда работу с кеше
# в него также можно перенест работу  сфильтром 


class ReestrData(object):
    def __init__(self):

    def get_cache(self, filter: str):
        data = _client()
        data.read_cache()


        data.get_record_count(filter)
        self.row_count = data.json_data["RawOlapSettings"]["lazyLoadOptions"]["limit"]

        with open('data/response.json', mode='r' ) as resp:
            cached = json.load(resp)
            
        

    def create_df(self, filter: str = 'oil'):
        """
        Метод для создания пандас-датафрейма из данных, полученных после запроса к реестру
        """
        self.types = {
            "date": "datetime64[D]",
            "Last": "bool",
            "N": "str",
            "E": "str",
            "prev_lic": "str",
            "prev_date": "datetime64[D]",
            "forw_lic": "str",
            "forw_date": "datetime64[D]",
            "name": "str",
            "type": "str",
            "state": "str",
            "INN": "str",
            "Year": "int",
        }

        data = self.get_data_from_reestr(filter)
        self.df = pd.DataFrame(data).set_index("num")
        
        # выделение столбца ГОД
        self.df["date"] = pd.to_datetime(
            self.df["date"], format="%Y-%m-%d", yearfirst=True
        )
        self.df["Year"] = self.df["date"].dt.year  # Год лицензии
        self.df["Year"] = self.df["Year"].astype('int')
        
        # Условный столбец последняя лицензия
        self.df["Last"] = np.where(
            self.df['forw_full'].isna(),
            True,
            False,
        )  

        # Извлечение ИНН:
        # Паттерны regrex
        pattern_for_inn = "([\d{10}|\d{12}]+)"
        pattern_for_replace_inn = "( \(ИНН.*\)$)"

        self.df["INN"] = self.df["owner_full"].str.extract(pattern_for_inn)
        self.df["INN"] = self.df["INN"].astype('int', errors="ignore")
        self.df["owner"] = self.df["owner_full"].str.replace(pattern_for_replace_inn, "", regex=True)


    # Функция для очистки данных о недропользователях
    # Функция изменяет ИНН на последний для дублкатов, когда несколько ИНН у одного наименования недропользовтаеля
    # Как праило это ликвидированные или реорганизованные недропользователи    
        def change_inn(owners):
            for owner in owners:
                try:
                    query = self.df.loc[(self.df['owner'] == owner),  ['INN','owner','Year']].sort_values(by='Year', ascending=False)
                    self.df.loc[self.df['owner'] == owner, 'INN'] = query.iloc[0,0]
                except Exception:
                    continue
        def change_owners(inns):
            for inn in inns:
                try:
                    query = self.df.loc[(self.df['INN'] == inn),  ['INN','owner','Year']].sort_values(by='Year', ascending=False)
                    self.df.loc[self.df['INN'] == inn, 'owner'] = query.iloc[0,1]
                except Exception:
                    continue

        change_inn(self.df['owner'].unique().tolist())
        change_owners(self.df['INN'].unique().tolist())

        self.df["owner"] = self.df["owner"].fillna(value=self.df["owner_full"], axis=0)
        
        forms = {'ЗАКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО':'ЗАО',
                    'ИНДИВИДУАЛЬНЫЙ ПРЕДПРИНИМАТЕЛЬ':'ИП',
                    'НЕПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО':'НПАО',
                    'ОБЩЕСТВО С ОГРАНИЧЕННОЙ ОТВЕТСТВЕННОСТЬЮ':'ООО',
                    'ОТКРЫТОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО':'ОАО',
                    'ПУБЛИЧНОЕ АКЦИОНЕРНОЕ ОБЩЕСТВО':'ПАО',
                    'ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ БЮДЖЕТНОЕ УЧРЕЖДЕНИЕ':'ФГБУ',
                    'ФЕДЕРАЛЬНОЕ ГОСУДАРСТВЕННОЕ УНИТАРНОЕ ГЕОЛОГИЧЕСКОЕ ПРЕДПРИЯТИЕ':'ФГУ ГП',
                    'АКЦИОНЕРНОЕ ОБЩЕСТВО':'АО',}

        self.df['owner'] = self.df['owner'].str.upper()
        for k, v in forms.items():
            self.df['owner'] = self.df['owner'].str.replace(pat=k, repl=v)

        # STEP PREV_FORW_LIC
        # Извлечение номеров предыдущих и будущих:
        pattern_for_lic = (
            "([А-Я]+[0-9]+[-А-Я]+)"  # Паттерн для извлечения номера ранее выданной лицензии
        )
        pattern_for_date = (
            "([\d\.]+$)"  # Паттерн для извлечения даты ранее выданной лицензии
        )

        # Извлечение данных предыдущих лицензий
        self.df["prev_lic"] = self.df["prew_full"].str.extract(pattern_for_lic)
        self.df["prev_date"] = self.df["prew_full"].str.extract(pattern_for_date)
        self.df["prev_date"] = pd.to_datetime(
            self.df["prev_date"], format="%d.%m.%Y", errors="ignore", dayfirst=True
        )

        # Извлечение данных будущих лицензий
        self.df["forw_lic"] = self.df['forw_full'].str.extract(pattern_for_lic)
        self.df["forw_date"] = self.df['forw_full'].str.extract(pattern_for_date)
        self.df["forw_date"] = pd.to_datetime(
            self.df["forw_date"], format="%d.%m.%Y", errors="ignore", dayfirst=True
        )

        # STEP COORDS
        # Извлечение координат:
        # Паттерн regrex
        pattern_coord = "(?P<N>\d*°\d*'[\d\.]+\"N)(?: *)(?P<E>[\d-]*°\d*'[\d\.]+\"[E|Е])"

        coords = (
            self.df['coords']
            .str.extractall(pattern_coord, re.MULTILINE)
            .sort_values(by="N", ascending=False)
            .droplevel(1)
        )
        coords = coords[coords.index.duplicated(keep="first") == False]

        # Объединение датафрейма
        self.df = pd.merge(self.df, coords, how="left", left_index=True, right_index=True)

        # Функция для извлечения радиан из столбцов N и E
        def to_rads(value) -> float:

            pattern = "(\d*)°(\d*)'(\d*\.*\d*)"
            val = re.findall(pattern=pattern, string=value)[0]
            return round(float(val[0]) + float(val[1])/60 + float(val[2])/3600, 5)

        # Добавление столбцов
        # Перевод координат град-мин-сек в радианы
        self.df.loc[:,'rad_N'] = self.df['N'].map(arg=to_rads, na_action='ignore')
        self.df.loc[:,'rad_E'] = self.df['E'].map(arg=to_rads, na_action='ignore')

        #Добавление периодов
        self.df.loc[:,'month'] = self.df['date'].dt.to_period('M')


        # STEP PREV_OWNER
        # Добавление столбца предыдущего владельца лицензии
        self.df = self.df.loc[~self.df.index.duplicated(keep="last")]  # Убираем дубликаты из индекса

        prev_owner_df = self.df.reset_index().set_index("forw_lic")
        prev_owner_df = prev_owner_df[prev_owner_df.index.notnull()]
        prev_owner_df = prev_owner_df[~prev_owner_df.index.duplicated(keep="last")][
            "owner"
        ].rename("prev_owner")

        # Присоединение столбца с данными о предыдущих владельцах
        self.df = pd.concat([self.df, prev_owner_df], axis=1, join="outer").rename_axis(
            "num"
        )
        self.df = self.df[self.df["date"].notna()]

        
    def save(self):
        
        excel_path = os.path.join(self.path, f'{self.filter}.xlsx')
        json_path = os.path.join(self.path, f'{self.filter}.zip')

        self.df = (self.df.drop(labels=list(_cols.values())[5:], axis=1)
            .astype(dtype=self.types)
            .where(~self.df.isnull(), "")
            .reset_index()
        )

        self.df.to_excel(excel_writer=excel_path,freeze_panes=(1, 0))
        self.df.to_json(path_or_buf=json_path, orient='records', indent=4, compression='zip')
        


    def create_matrix(self):
        """
        Метод создает матрицу для создания меппенига для ГБЗ. Перед применением метода создай датафрейм .create_df(filter)
        """
        dataset = self.df 
        #Очистить данные о предыдущих лицензиях
        dataset['prew_'] = dataset['prew_full'].str.replace('\n', ':', regex=True).str.replace(' от \d\d\.\d\d', '', regex=True)

        #Извлечь все предыдущие лицензии и их год
        pattern = "(?P<old_lic>[А-Я]{3}\d*[А-Я]{2})(?:.)(?P<old_year>\d{4})"
        df_left = dataset['prew_'].str.extractall(pattern).droplevel(1).astype(dtype={'old_year':'int'})

        #Добавить все текущие лицензии и их год
        df_right = dataset.loc[dataset['Last'] == True, ['Year']].astype('int')

        #Таблица для связи текущих лицензий с предыдущими
        lookup_table = df_left.merge(df_right, how='right', left_index=True, right_index=True)
        lookup_table = lookup_table[['Year', 'old_lic', 'old_year']]    #.to_excel(excel_writer='data/test.xlsx')

        #Создание датафрейма для матрицы соотношения номеров старых лицензий с текущими лицензиями
        index = list(set(lookup_table.index.to_list()))
        columns = list(set(lookup_table.Year.tolist()))

        matrix = pd.DataFrame(data = None, index = index, columns=columns)
        matrix

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
        
        self.matrix_path = os.path.join(self.path, 'matrix.xlsx')
        if os.path.exists(self.matrix_path):
            with pd.ExcelWriter(path=self.matrix_path, if_sheet_exists='replace', mode='a') as writer:
                matrix.to_excel(writer, sheet_name='matrix')
                lookup_table.to_excel(writer, sheet_name='lookup_table')
        else: 
            matrix.to_excel(self.matrix_path, sheet_name='matrix')
            lookup_table.to_excel(self.matrix_path, sheet_name='lookup_table')