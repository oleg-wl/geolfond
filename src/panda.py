import pandas as pd
import numpy as np
import re
import os

from src.app import ReestrRequest

class ReestrData(ReestrRequest):
    def __init__(self, filter='oil'):
        super().__init__(filter='oil')

        self.config()
        self.get_data_from_reestr()
        self.df = pd.DataFrame(data=self.data)


    def create_df(self):
        """
        Метод для создания пандас-датафрейма из данных, полученных после запроса к реестру
        """
        # Переименовать столбцы
        cols = {
            'Государственный регистрационный номер':'num',
            'Дата':'date',
            'Вид полезного ископаемого':'type',
            'Наименование субъекта Российской Федерации или иной территории, на которой расположен участок недр':'state',
            'Наименование участка недр, предоставленного в пользование по лицензии, кадастровый номер месторождения или проявления полезных ископаемых в ГКМ':'name',
            #дропнуть
            'Сведения о пользователе недр':'owner_full',
            'Географические координаты угловых точек участка недр, верхняя и нижняя границы участка недр':'coords',
            'Сведения о переоформлении лицензии на пользование недрами':'forw_full',
            'Сведения о реестровых записях в отношении ранее выданных лицензий на пользование соответствующим участком недр':'prew_full',
            'Реквизиты документа, на основании которого выдана лицензия на пользование недрами':'doc_details',
            'Наименование органа, выдавшего лицензию':'agency',
            'Статус участка недр':'status',
            'Целевое назначение лицензии':'purpose',
            'Наличие полного электронного образа':'image',
            'Ссылка на карточку лицензии':'link',
            'Сведения о внесении изменений и дополнений в лицензию на пользование недрами, сведения о наличии их электронных образов':'changes',
            'Реквизиты приказа о прекращении права пользования недрами, приостановлении или ограничении права пользования недрами':'order',
            'Дата.1':'date_stop',
            'Дата.2':'date_end',
            'Срок и условия приостановления или ограничения права пользования недрами':'stop_end_conditions',
            'Ссылка на АСЛН':'link_alsn'
            }
        types = {
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
        
        self.df = self.df.rename(columns=cols).set_index("num")
        
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

        for_test = self.df

        self.df = (self.df.drop(columns=list(cols.values())[5:])
            .astype(dtype=types)
            .where(~self.df.isnull(), "")
            .reset_index()
        )

        return for_test
        
    def save(self):
        self.create_df()
        
        excel_path = os.path.join(self.path, f'{self.filter}.xlsx')
        json_path = os.path.join(self.path, f'{self.filter}.zip')

        self.df.to_excel(excel_writer=excel_path,freeze_panes=(1, 0))
        self.df.to_json(path_or_buf=json_path, orient='records', indent=4, compression='zip')
        