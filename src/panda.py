import pandas as pd
import numpy as np
import re
import os


def construct_df(nested_data: dict):
    """
    Функция для создания пандас-датафрейма и обработки словаря, полученного после запроса к реестру. Возвращает датафрейм для сохранения.
    """
    # Переменные столбцов и типов для возврата датафрейма.
    cols = [
        "Дата лицензии",
        "СубъектРФ",
        "Вид полезного ископаемого",
        "Наименование участка недр",
        "Сведения о пользователе недр",
        "INN",
        "Наименоваение недропользователя",
        "prev_owner",
        "Last",
        "Year",
        #"Координаты",
        "N", #Самая северная точка участка для определения применимости налоговых льгот
        "E",
        #"Сведения о переоформлении лицензии на пользование недрами",
        #"Ранее выданные лицензии",
        "prev_lic",
        "prev_date",
        "forw_lic",
        "forw_date",
    ]
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
    df["Year"] = pd.to_datetime(
        df["Дата лицензии"], format="%Y-%m-%d", yearfirst=True
    ).dt.year  # Год лицензии
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
    df["Наименоваение недропользователя"] = (
        df["Сведения о пользователе недр"]
        .replace(pattern_for_replace_inn, "", regex=True)
        .str.strip()
    )

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

    # STEP PREV_OWNER-----------
    # Добавление столбца предыдущего владельца лицензии
    df = df.loc[~df.index.duplicated(keep="last")]  # Убираем дубликаты из индекса

    prev_owner_df = df.reset_index().set_index("forw_lic")
    prev_owner_df = prev_owner_df[prev_owner_df.index.notnull()]
    prev_owner_df = prev_owner_df[~prev_owner_df.index.duplicated(keep="last")][
        "Наименоваение недропользователя"
    ].rename("prev_owner")

    #TODO: Добавить выгрузку для столбца с будущим недропользователем
    #TODO: forw_owner_df и добавить строку конкат

    # Присоединение столбца с данными о предыдущих владельцах
    df = pd.concat([df, prev_owner_df], axis=1, join="outer").rename_axis(
        "Государственный регистрационный номер"
    )
    df = df[df["Дата лицензии"].notna()]

    #TODO: Сделать столбец с будущими владельцами.

    # STEP PREV_NAMES-----------
    for i in range(100):
        names_df = df.reset_index().set_index("prev_lic")
        names_df = names_df[names_df.index.notnull()]

        # Лист лицензий из names_df:
        lst = names_df.index.unique().tolist()
        # Лист лицензий из основного ДФ у которых нет названий но есть названия в names_df:
        lst2 = df.index[
            df.index.isin(lst) & df["Наименование участка недр"].isna()
        ].tolist()

        # Замена значений названий:
        df.loc[df.index.isin(lst2), ["Наименование участка недр"]] = names_df.loc[
            names_df.index.isin(lst2)
        ]

    df = (
        df[cols]
        .astype(dtype=types)
        .where(
            ~df.isnull(), ""
        )  # заменить значения NaN на пустые для выгрузки в эксель
    )
    return df


def construct_pivot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Функция для создания матрицы ключей для связки с ГБЗ. Создает плоскую таблицу с столбцами год и номерами предыдущих лицензий.

    :return pd.DataFrame: датафрейм для сохранения в эксель
    """

    # Создание датафрейма с номерами послединих лицензий
    df_last = df[
        [
            "Наименование участка недр",
            "Государственный регистрационный номер",
            "Дата лицензии",
            "Year",
        ]
    ][df.Last == True]
    df_last["prev_lic"] = df_last["Государственный регистрационный номер"]

    # Создание датафрейма для присоединения к мейну
    df_last_pivoted = (
        df_last.pivot(
            index=[
                "Наименование участка недр",
                "Государственный регистрационный номер",
                "Дата лицензии",
            ],
            columns=["Year"],
        )["prev_lic"]
        .sort_values(by=["Year"], axis=1, ascending=False)
        .bfill(axis=1)
        .ffill(axis=1)
    )

    # Создание основного датафрейма для лицензий
    df_main = df[
        [
            "Наименование участка недр",
            "Государственный регистрационный номер",
            "Дата лицензии",
            "Year",
            "prev_lic",
        ]
    ].sort_values("Year", ascending=False)

    # добавление "_" к значению
    df_main["prev_lic"] = df_main["prev_lic"].apply(
        lambda x: "_" + x if type(x) is str else np.nan
    )

    # Создание пивот-датафрейма с столбцами в убывающем порядке
    df_pivoted = (
        df_main.pivot(
            index=[
                "Наименование участка недр",
                "Государственный регистрационный номер",
                "Дата лицензии",
            ],
            columns=["Year"],
        )["prev_lic"]
        .sort_values(by=["Year"], axis=1, ascending=False)
        .ffill(axis=1)
    )

    # Объединение датафреймов main и last. Датафрейм last это дополнительная строка последнеей записи в реестре
    df_concat = pd.concat([df_pivoted, df_last_pivoted]).sort_index(level=[0, 2])

    return df_concat


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
