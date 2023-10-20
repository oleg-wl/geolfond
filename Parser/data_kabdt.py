import os
import pandas as pd
from bs4 import BeautifulSoup as bs
from pretty_html_table import build_table

from .data_transformer import DataTransformer
from .reestr_config import getlogger

_logger = getlogger("Kdemp")


class DataKdemp(DataTransformer):
    def __init__(self, data: [str | list | dict] = None) -> None:
        super().__init__(data)

    def create_abdt_index(self):
        """
        Метод для расчета коэф Цабвр и Цдтвр на основе данных Спббиржи
        print(self.abdt)

        :raises ValueError: если нет данных для обработки. Передай аргументу data конструктора результат client.get_abdt_index
        """

        if (self.data is None) or (not isinstance(self.data, dict)):
            _logger.error(
                "Нет данных для обработки. Передай результат client.get_abdt_index"
            )
            raise ValueError()

        def prep_vals(df: pd.DataFrame) -> pd.DataFrame:
            """
            Функция для создания датафрейма из исходных данных биржи

            :param pd.DataFrame df: датафрейм из self.data
            """
            df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d", dayfirst=True)
            df = df[["date", "value"]].dropna()

            return df

        # Бензин
        df_regs = prep_vals(pd.read_csv(self.data["reg"], delimiter=";"))

        # Дизель
        dt_inds = ["dtl", "dtm", "dtz"]
        l = []
        for i in dt_inds:
            dt = prep_vals((pd.read_csv(self.data[i], delimiter=";")))
            l.append(dt)
        df_dt = pd.concat(l)

        # обработка инстанса без группировки для картинки
        self.ab_nogroup = df_regs
        self.dt_nogroup = df_dt

        return self

    def abdt_index_cumulative(self) -> None:
        """
        Метод для сохранения в эксель накопительных средних цен АБ92 и ДТ
        """

        def meaning(df: pd.DataFrame):
            # Функция для добавления столбца с накопленной средней в датафрейм

            df["month"] = df["date"].dt.to_period("M")  # Добавить столбец с месяцами

            mnt_list = set(df["month"].to_list())
            for mnt in mnt_list:
                mask = df["month"] == mnt
                l = [
                    df[mask].iloc[0 : r + 1, 1].mean(numeric_only=True)
                    for r in range(len(df[mask]))
                ]
                df.loc[mask, "mean"] = l
            df["mean"] = df["mean"].round()
            return df.sort_values(by="date", ascending=False)

        # Средняя кумулятивная цена бензина
        ab = meaning(self.ab_nogroup).reset_index(drop=True)

        # Средняя кумулятивная дизеля
        # NOTE: по дизелю собирается один датафрейм из трех индексов. Ниже группировка по дням для средней.
        # Для того чтобы функция meaning возвращала корректные цифры - не меняй сортировку
        # Конечная сортировка в функции
        dt = (
            self.dt_nogroup.set_index("date")
            .groupby(pd.Grouper(freq="D"))
            .mean("value")
            .reset_index()
        )
        dt = meaning(dt)
        m = ab.merge(dt, how="inner", on="date", suffixes=("_АБ", "_ДТ")).reset_index(
            drop=True
        )

        m["date"] = m["date"].dt.strftime("%d-%m-%Y")

        _logger.info("Сохранил среднюю")

        #:анкоммент для сохранения в эксель
        m.to_excel(os.path.join(self.path, "СредняяЦенаАБДТ_накоп.xlsx"))
        self.abdt = m
        return self

    def kdemp(self) -> str:
        """
        Метод возвращает строку html таблицы со средними показателями для расчета Кдемп и Кабдт для отправки по электронной почте или для телеграм бота

        :raises ValueError: должны быть данные в self.abdt
        :return str: html таблица для отправки по почте
        """
        self.create_abdt_index()
        self.abdt_index_cumulative()

        if self.abdt is None:
            raise ValueError("Нет данных для расчета Кдемп")

        # [Цаб_вр, Цдт_вр] по годам
        def add_ind():
            # функция добавляет индикатив
            match self.y:
                case 2023:
                    ab, dt = 56900, 53850
                case 2024:
                    ab, dt = 58650, 55500
                case 2025:
                    ab, dt = 60450, 57200
                case 2026:
                    ab, dt = 62300, 58950
                case _:
                    raise ValueError("Ошибка в значении текущего года")
            return int(ab * 1.1), int(dt * 1.2)

        self.abdt["Бензин92_норматив"], self.abdt["Дизель_норматив"] = add_ind()

        self.abdt = self.abdt[
            (self.abdt["month_АБ"].dt.month == self.m)
            & (self.abdt["month_АБ"].dt.year == self.y)
        ]

        # html для вставки демфера
        dt = self.abdt.loc[0, "date"]
        delt_ab = self.abdt.loc[0, "mean_АБ"] - self.abdt.loc[0, "Бензин92_норматив"]
        delt_dt = self.abdt.loc[0, "mean_ДТ"] - self.abdt.loc[0, "Дизель_норматив"]
        color = "red" if (delt_ab >= 0) or (delt_dt >= 0) else "green"

        demp = (
            f'<b style="color: red;">НЕТ</b>'
            if (delt_ab >= 0) or (delt_dt >= 0)
            else f'<b style="color: green;">ДА</b>'
        )
        demp_num = f'АБ: <b style="color: {color};">{delt_ab*-1:n}</b> руб./тн; ДТ: <b style="color: {color};">{delt_dt*-1:n}</b> руб./тн'

        names = {
            "date": "Дата",
            "value_АБ": "АБ",
            "mean_АБ": "Средняя_АБ",
            "Бензин92_норматив": "Бензин92_норматив",
            "value_ДТ": "ДТ",
            "mean_ДТ": "Средняя_ДТ",
            "Дизель_норматив": "Дизель_норматив",
        }

        types = {"АБ": "int", "ДТ": "int", "Средняя_АБ": "int", "Средняя_ДТ": "int"}

        self.abdt.rename(columns=names, inplace=True)

        self.abdt = self.abdt[list(names.values())].round().astype(types)
        self.abdt.to_excel(os.path.join(self.path, "СредняяЦенаАБДТ.xlsx"), index=False)

        # добавить пробелы цифрам через реплейс
        cols = [
            "АБ",
            "ДТ",
            "Средняя_АБ",
            "Средняя_ДТ",
            "Бензин92_норматив",
            "Дизель_норматив",
        ]
        for c in cols:
            self.abdt[c] = self.abdt[c].apply(
                lambda x: "{:,}".format(x).replace(",", " ")
            )

        # убрать значения для rowspan
        self.abdt.loc[1:, "Бензин92_норматив"] = ""
        self.abdt.loc[1:, "Дизель_норматив"] = ""

        # html w pretty-html-table
        s1 = build_table(
            self.abdt,
            "grey_light",
            index=False,
            font_family="Calibri",
            font_size="13px",
            width="100px",
            text_align="center",
        )

        s0 = f'<p>Добрый день!<br>Направляем текущие средние цены АБ и ДТ внутреннего рынка для расчета Кдемп по итогам торгов {dt}</p> <p>Прогноз получения демпфера в этом месяце - {demp}</p><p>Запас цены до норматива*: {demp_num}</p><p><a href="https://blps-datalab.gazprom-neft.local/sense/app/b334752d-ee58-46db-8ff0-764b6ea10a3c/sheet/bb5a5146-03b7-43a1-8d8b-48de26d71c8e/state/analysis"> Посмотреть динамику на Дашборде </a></p>'

        s2 = "<p><em>*Если хотя бы по одному из видов топлива средняя цена по итогам месяца будет превышать норматив, то демпфер обнуляется как по АБ, так и по ДТ</em></p>"

        return s0 + s1 + s2

    def soup_html(self, html) -> str:
        page = bs(html, "lxml")

        page.tbody.find_all("td")[3]["rowspan"] = len(page.tbody.tr) - 1
        page.tbody.find_all("td")[6]["rowspan"] = len(page.tbody.tr) - 1

        # td с индикативом для добавления атрибута rowspan по количеству строк (дней) в таблице
        for i in page.tbody.find("tr").find_all("td"):
            if (i.string == "62 590") or (i.string == "64 620"):
                i["rowspan"] = len(page.tbody.tr)

        # убрать пустые td для корректного rowspan
        for r in page.tbody.find_all("td"):
            if r.string is None:
                r.extract()

        return bs.prettify(page)
