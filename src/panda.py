import pandas as pd
import numpy as np
import re
import os
import sys


def construct_df(nested_data: dict):
    """
    Функция для создания пандас-датафрейма и обработки словаря, полученного после запроса к реестру. Возвращает датафрейм для сохранения.
    """
    # Переменные столбцов и типов для возврата датафрейма.
    cols = {
        "Государственный регистрационный номер":'num',
        "Дата лицензии":'date',
        "СубъектРФ":'state',
        "Вид полезного ископаемого":'type',
        "Наименование участка недр":'name',
        #"Сведения о пользователе недр",
        "INN":"INN",
        "owner":'owner',
        "prev_owner":"prew_owner",
        "Last":"Last",
        "Year":"Year",
        #"Координаты",
        #"Сведения о переоформлении лицензии на пользование недрами",
        #"Ранее выданные лицензии",
        "prev_lic":"prev_lic",
        "prev_date":"prev_date",
        "forw_lic":"forw_lic",
        "forw_date":'forw_date',
        "N":"N", #Самая северная точка участка для определения применимости налоговых льгот
        "E":"E",
        "rad_N":"rad_N",
        "rad_E":"rad_E",
        "month":"month"
    }
    types = {
        "Дата лицензии": "datetime64[D]",
        "Last": "bool",
        "N": "str",
        "E": "str",
        "prev_lic": "str",
        "prev_date": "datetime64[D]",
        "forw_lic": "str",
        "forw_date": "datetime64[D]",
        "Наименование участка недр": "str",
        "Вид полезного ископаемого": "str",
        "СубъектРФ": "str",
        "INN": "str",
        "Year": "int",
    }

    df = (
        pd.DataFrame(nested_data)
        .rename(
            columns={
                "Дата": "Дата лицензии",
                "Дата.1": "Дата прекращения лицензии",
                "Дата.2": "Дата окончания лицензии",
                "Географические координаты угловых точек участка недр, верхняя и нижняя границы участка недр": "Координаты",
                "Наименование органа, выдавшего лицензию": "Орган",
                "Сведения о реестровых записях в отношении ранее выданных лицензий на пользование соответствующим участком недр": "Ранее выданные лицензии",
                "Реквизиты приказа о прекращении права пользования недрами, приостановлении или ограничении права пользования недрами": "Реквизиты прекращения, приостановления, ограничения",
                "Наименование участка недр, предоставленного в пользование по лицензии, кадастровый номер месторождения или проявления полезных ископаемых в ГКМ": "Наименование участка недр",
                "Наименование субъекта Российской Федерации или иной территории, на которой расположен участок недр": "СубъектРФ",
            }
        )
        .set_index("Государственный регистрационный номер")
    )
    
    # выделение столбца ГОД
    df["Дата лицензии"] = pd.to_datetime(
        df["Дата лицензии"], format="%Y-%m-%d", yearfirst=True
    )
    df["Year"] = df["Дата лицензии"].dt.year  # Год лицензии
    df["Last"] = np.where(
        df["Сведения о переоформлении лицензии на пользование недрами"].isna(),
        True,
        False,
    )  # Условный столбец последняя лицензия

    # STEP INN-----------
    # Извлечение ИНН:
    # Паттерны regrex
    pattern_for_inn = "([\d{10}|\d{12}]+)"
    pattern_for_replace_inn = "(\(ИНН.*\)$)"

    df["INN"] = df["Сведения о пользователе недр"].str.extract(pattern_for_inn)
    df["INN"] = (
        df["INN"].astype(str, errors="ignore").str.replace(".0", "", regex=False)
    )
    df["owner"] = (
        df["Сведения о пользователе недр"]
        .replace(pattern_for_replace_inn, "", regex=True)
        .str.strip()
    )

# Функция для очистки данных о недропользователях
# Функция изменяет ИНН на последний для дублкатов, когда несколько ИНН у одного наименования недропользовтаеля
# Как праило это ликвидированные или реорганизованные недропользователи    
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

    change_inn(df['owner'].unique().tolist())
    change_owners(df['INN'].unique().tolist())

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

    # STEP PREV_FORW_LIC-----------
    # Извлечение номеров предыдущих и будущих:
    pattern_for_lic = (
        "([А-Я]+[0-9]+[-А-Я]+)"  # Паттерн для извлечения номера ранее выданной лицензии
    )
    pattern_for_date = (
        "([\d\.]+$)"  # Паттерн для извлечения даты ранее выданной лицензии
    )

    # Извлечение данных предыдущих лицензий
    df["prev_lic"] = df["Ранее выданные лицензии"].str.extract(pattern_for_lic)
    df["prev_date"] = df["Ранее выданные лицензии"].str.extract(pattern_for_date)
    df["prev_date"] = pd.to_datetime(
        df["prev_date"], format="%d.%m.%Y", errors="ignore", dayfirst=True
    )

    # Извлечение данных будущих лицензий
    df["forw_lic"] = df[
        "Сведения о переоформлении лицензии на пользование недрами"
    ].str.extract(pattern_for_lic)
    df["forw_date"] = df[
        "Сведения о переоформлении лицензии на пользование недрами"
    ].str.extract(pattern_for_date)
    df["forw_date"] = pd.to_datetime(
        df["forw_date"], format="%d.%m.%Y", errors="ignore", dayfirst=True
    )

    # STEP COORDS-----------
    # Извлечение координат:
    # Паттерн regrex
    pattern_coord = "(?P<N>\d*°\d*'[\d\.]+\"N)(?: *)(?P<E>[\d-]*°\d*'[\d\.]+\"[E|Е])"

    coords = (
        df["Координаты"]
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

    #Добавление периодов
    df.loc[:,'month'] = df['Дата лицензии'].dt.to_period('M')


    # STEP PREV_OWNER-----------
    # Добавление столбца предыдущего владельца лицензии
    df = df.loc[~df.index.duplicated(keep="last")]  # Убираем дубликаты из индекса

    prev_owner_df = df.reset_index().set_index("forw_lic")
    prev_owner_df = prev_owner_df[prev_owner_df.index.notnull()]
    prev_owner_df = prev_owner_df[~prev_owner_df.index.duplicated(keep="last")][
        "owner"
    ].rename("prev_owner")

    # Присоединение столбца с данными о предыдущих владельцах
    df = pd.concat([df, prev_owner_df], axis=1, join="outer").rename_axis(
        "Государственный регистрационный номер"
    )
    df = df[df["Дата лицензии"].notna()]

    df = (
        df[list(cols.keys())[1:]]
        .astype(dtype=types)
        .where(~df.isnull(), "")
        .reset_index()
        .rename(columns=cols)
    )
    return df


def save_in_sql(table, df: pd.DataFrame, connect: str):
    df = (
        df.reset_index()
        .astype(dtype="str")
        .to_sql(name=table, con=connect, if_exists="replace")
    )


def save_in_excel(path: str, dataframe_to_save: pd.DataFrame, name_for_sheet: str):
    """
    Функция для сохранения в определенный лист экселя
    path - путь для сохранения
    df - датафрейм для сохранения
    name_for_sheet - название листа
    """

    if os.path.exists(path):
        with pd.ExcelWriter(path, mode="a", if_sheet_exists="replace") as writer:
            dataframe_to_save.to_excel(
                writer,
                sheet_name=name_for_sheet,
                merge_cells=False,
                freeze_panes=(1, 0),
                na_rep="",
            )

    else:
        dataframe_to_save.to_excel(
            path,
            sheet_name=name_for_sheet,
            merge_cells=False,
            freeze_panes=(1, 0),
            na_rep="",
        )

def path_to_desktop():
    # Функция для определения пути сохранения файла на рабочий стол в зависимости от ОС

    ostype = sys.platform
    if "win" in ostype:
        desktop = os.path.join(os.path.join(os.environ["USERPROFILE"]), "Desktop")
    else:
        desktop = os.path.expanduser("~")
    return desktop