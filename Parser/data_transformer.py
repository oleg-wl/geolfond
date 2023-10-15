#!venv/bin/python3
import re
import os
import datetime

import pandas as pd
import numpy as np

from .reestr_config import logger, check_path

class DataTransformer:
    y = datetime.datetime.now().year
    m = datetime.datetime.now().month
    
    def __init__(self, data: [str|list|dict] = None) -> None:
        
        self.logger = logger()
        self.path = check_path()

        #Сделать переменные для обработки при инициализации класса
        # и return self 
        self.data = data #передача данных в класс для обработки
        self.rosnedra = None #переменная для хранения результата обработки
        self.abdt = None #переменная для хранения абдт индекса
        
        

    def create_df(self) -> pd.DataFrame: 
        """
        Очистка ответа API с помощью библиотеки pandas. Функция возвращает обработанный датафрейм.

        :param list raw_data: список словарей ответа api
        :return pd.DataFrame: очищенные данные
        """
        #Input check
        if (self.data is None) or (not isinstance(self.data, list)):
            raise ValueError('Отсутствуют даннанные для обработки. Нужен список, который возвращает client().get_data_from_reestr ')
        
        types = {
            "date": 'datetime64[ns]',
            "Year": "int",
            'month': "str",
            "name": "str",
            "INN": "str",
            'owner': 'str',
            "N": "str",
            "E": "str",
            'rad_N':'float',
            "rad_E":'float',
            "prev_lic": "str",
            "prev_date": 'datetime64[ns]',
            "prev_owner":"str",
            "forw_lic": "str",
            "forw_date": 'datetime64[ns]',
            "type": "str",
            "state": "str",
            "Last": "bool",
            "status":'str',
            "filter": 'str'
        }

        df = pd.DataFrame(self.data).set_index("num")
        
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
        pattern_for_inn = '(\d{10,12})'
        pattern_for_replace_inn = "( \(ИНН.*\)$)"
        patt_status = '(Аннулирование|Приостановление|Ограничение)'

        df["INN"] = df["owner_full"].str.extract(pattern_for_inn)
        df["INN"] = df["INN"].astype('int', errors="ignore")
        df["owner"] = df["owner_full"].str.replace(pattern_for_replace_inn, "", regex=True)
        df["owner"] = df["owner"].str.replace('\"|\'|«|»', "", regex=True)
        
        #Анулированные лицензии
        df['status'] = df['ordered'].str.extract(patt_status)

        #: Очистка для столбца с видом полезного ископаемого
        df['filter'] = df['filter'].str.slice(start=4) 

        #: Функция для очистки данных о недропользователях
        #: Функция изменяет ИНН на последний для дублкатов, когда несколько ИНН у одного наименования недропользовтаеля
        #: Как праило это ликвидированные или реорганизованные недропользователи    
        #! Временно отключено для проверки совместимости данных
        #! и ускорения процесса загрузки
        #def change_inn(owners):
        #    for owner in owners:
        #        try:
        #            query = df.loc[(df['owner'] == owner),  ['INN','owner','Year']].sort_values(by='Year', ascending=False)
        #            df.loc[df['owner'] == owner, 'INN'] = query.iloc[0,0]
        #        except IndexError:
        #            continue
        #def change_owners(inns):
        #    for inn in inns:
        #        try:
        #            query = df.loc[(df['INN'] == inn),  ['INN','owner','Year']].sort_values(by='Year', ascending=False)
        #            df.loc[df['INN'] == inn, 'owner'] = query.iloc[0,1]
        #        except IndexError:
        #            continue

        #: Оптимизация для фильтров не oil
        
        #if df['filter'].str.contains('Углеводородное сырье').any():
        #    self.logger.debug(f'Очистка данных о недропользователях')
        #    change_inn(df['owner'].unique().tolist())
        #    change_owners(df['INN'].unique().tolist())
        #    self.logger.debug(f'Очистка данных завершена')

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

        #: Заменить все nan и 0 в координатах
        #df = df.replace(to_replace=['nan'], value=pd.NA)

        #: Тесты
        try: 
            assert df['prew_full'].count() == df['prev_lic'].count(), f"Тест не пройден, prew_lic:{df['prev_lic'].count()}, prew_full:{df['prew_full'].count()}"

            assert df['forw_full'].count() == df['forw_lic'].count(), f"Тест не пройден, forw_lic: {df['forw_lic'].count()}, forw_full: {df['forw_full'].count()}"

            assert (df['N'].count() == df['coords'].count()) & (df['E'].count() == df['coords'].count()), f"Тест не пройден, N: {df['N'].count()}, E: {df['E'].count()}, coords: {df['coords'].count()}"

            assert df['owner'].count() == df['owner_full'].count(), f"Тест не пройден, owner: {df['owner'].count()}, owner full: {df['owner_full'].count()}"

            self.logger.info(f'Тесты ОК, всего строк: {len(df.index)}')

        except AssertionError as err:
            self.logger.warning(err)
        finally:

            #Датафрейм для сохранения или передачи методу create_matrix
            self.rosnedra = df[list(types.keys())].reset_index()

        return self 
    
    def create_matrix(self) -> None:
        """
        Метод создает матрицу для меппенига данныз из ГБЗ и из Росгеолфонда
        Сохраняет в эсксель таблицу _matrix.xlsx
        """
        self.logger.info('Создаю матрицу')

        if self.rosnedra is None:
            raise ValueError('Нет данных для матрицы.')

        #Очистить данные о предыдущих лицензиях
        self.rosnedra['prew_'] = self.rosnedra['prew_full'].str.replace('\n', ':', regex=True).str.replace(' от \d\d\.\d\d', '', regex=True)

        #Извлечь все предыдущие лицензии и их год
        pattern = "(?P<old_lic>[А-Я]{3}\d*[А-Я]{2})(?:.)(?P<old_year>\d{4})"
        df_left = self.rosnedra['prew_'].str.extractall(pattern).droplevel(1).astype(dtype={'old_year':'int'})

        #Добавить все текущие лицензии и их год
        df_right = self.rosnedra.loc[self.rosnedra['Last'] == True, ['Year']].astype('int')

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
        
        matrix_path = os.path.join(self.path, '_matrix.xlsx')
        if os.path.exists(matrix_path):
            with pd.ExcelWriter(path=matrix_path, if_sheet_exists='replace', mode='a') as writer:
                matrix.to_excel(writer, sheet_name='matrix')
                lookup_table.to_excel(writer, sheet_name='lookup_table')
        else: 
            matrix.to_excel(matrix_path, sheet_name='matrix')
            with pd.ExcelWriter(path=matrix_path, if_sheet_exists='replace', mode='a') as writer:
                lookup_table.to_excel(writer, sheet_name='lookup_table')

    def create_prices(self, curr: dict, pr: dict) -> None:
        """
        Метод для обработки данных о ценах. Возвращает среднемесячную котировку Аргус и среднемесячный курс $ЦБ
        Сохраняет в prices.xlsx

        :param dict curr: Словарь с выгрузкой Аргуса client.get_oil_price
        :param dict pr: Словарь с курсом ЦБ client.get_currency
        """

        df_pr = pd.DataFrame(pr).set_index('Dates')
        df_curr = pd.DataFrame(curr)

        #: Обработка датафрейма с котировками Argus
        df_pr['Price'] = pd.to_numeric(df_pr['Price'].str.replace(',','.'))

        #: Обработка датафрейма с курсом ЦБ
        df_curr['Dates'] = pd.to_datetime(df_curr['Dates'], format='%d.%m.%Y', dayfirst=True)
        df_curr['Rate'] = df_curr['Rate'].str.replace(',', '.')
        df_curr['Rate'] = df_curr['Rate'].str.replace(' ', '')
        df_curr['Rate'] = pd.to_numeric(df_curr['Rate']) 

        #: группировка и Join
        m = df_curr.set_index('Dates').groupby(pd.Grouper(freq='MS')).mean().round(4)
        self.prices = df_pr.merge(m, how='inner', left_index=True, right_index=True)
        
        return self
        
    def create_abdt_index(self):
        """
        Метод для расчета коэф Цабвр и Цдтвр на основе данных Спббиржи
        print(self.abdt)  

        :raises ValueError: если нет данных для обработки. Передай аргументу data конструктора результат client.get_abdt_index
        :return _type_: self.abdt для метода kdemp
        """

        if (self.data is None) or (not isinstance(self.data, dict)):
            raise ValueError('Нет данных для обработки. Передай результат client.get_abdt_index')

        def prep_vals(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
            """
            Функция для создания датафрейма из исходных данных биржи

            :param pd.DataFrame df: датафрейм из self.data
            :return tuple[pd.DataFrame, pd.Dataframe]: возвращает два дф. первый с текущими данными, второй с данными минус 1 день для вычетания отклонения
            """
            df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d', dayfirst=True)
            df = df[['date', 'value']].dropna()
            df_y = df.drop(df.tail(1).index) #Датафрейм без последнего дня для расчета показателя изменений

            return (df, df_y)

        def mean_vals(df: pd.DataFrame) -> pd.DataFrame:
            #Средние значения цены для каждого месяца
            means = (df.set_index('date')
                     .groupby(pd.Grouper(freq='MS'))
                     .mean()
                     .round()
                     .sort_index(ascending=False))
            return means
        
        #Бензин
        df_regs = prep_vals(pd.read_csv(self.data['reg'], delimiter=';'))
        
        df_reg = mean_vals(df_regs[0])
        #_y предыдущий день
        df_reg_y = mean_vals(df_regs[1])

        #Дизель
        dt_inds = ['dtl', 'dtm', 'dtz']
        l = []
        l_y = []
        for i in dt_inds:
            dt = prep_vals((pd.read_csv(self.data[i], delimiter=';')))
            l.append(dt[0]) 
            l_y.append(dt[1]) 

        df_dt = pd.concat(l)
        # _предыдущий день
        df_dt_y = pd.concat(l_y)

        #обработка инстанса без группировки для картинки
        #TODO: группировка с начала года помесячно кумулятив
        self.ab_nogroup = df_regs[0]
        self.dt_nogroup = df_dt

        df_dt = mean_vals(df_dt)
        df_dt_y = mean_vals(df_dt_y)

        #Свести в единый датафрейм цену на бензин рег-92 и на дизель 
        md = df_reg.merge(df_dt, left_index=True, right_index=True, how='inner', suffixes=('_reg92', '_disel'))
        md_y = df_reg_y.merge(df_dt_y, left_index=True, right_index=True, how='inner', suffixes=('_reg92', '_disel'))


         
        self.abdt = md
        self.abdt_y = md_y
        return self

    def abdt_index_cumulative(self) -> None:
        """
        Метод для сохранения в эксель накопительных средних цен АБ92 и ДТ
        """

        def meaning(df: pd.DataFrame):
            # Функция для добавления столбца с накопленной средней в датафрейм
        
            df['month'] = df['date'].dt.to_period("M") # Добавить столбец с месяцами 
        
            mnt_list = set(df['month'].to_list())
            for mnt in mnt_list:
                mask = df['month'] == mnt
                l = [df[mask].iloc[0:r+1, 1].mean(numeric_only=True) for r in range(len(df[mask]))]
                df.loc[mask, 'mean'] = l
            df['mean'] = df['mean'].round()# .astype(int)
            return df.sort_values(by='date', ascending=False)

        ab = meaning(self.ab_nogroup).reset_index(drop=True)

        #собираем среднюю
        dt = self.dt_nogroup.sort_values('date', ascending=False).set_index('date').groupby(pd.Grouper(freq='D')).mean('value').sort_index(ascending=False).reset_index().round()
        dt = meaning(dt)
        m = ab.merge(dt, how='inner', on='date', suffixes=('_АБ', '_ДТ'))

        m['date'] = m['date'].dt.strftime('%d-%m-%Y')
        
        m.to_excel(os.path.join(self.path, 'СредняяЦенаАБДТ_накоп.xlsx'))
        self.logger.info('Сохранил среднюю')

    def kdemp(self) -> str:
        """
        Метод возвращает строку html таблицы со средними показателями для расчета Кдемп и Кабдт для отправки по электронной почте или для телеграм бота

        :raises ValueError: должны быть данные в self.abdt
        :return str: html таблица для отправки по почте
        """
        self.create_abdt_index()
        self.abdt_index_cumulative()

        if self.abdt is None:
            raise ValueError('Нет данных для расчета Кдемп')

        # [Цаб_вр, Цдт_вр] по годам
        # Указано к в НК в текущей редакции. Править тут руками в случае изменений в НК
        if self.y == 2023:
            norm = [56900, 53850]
        elif self.y == 2024:
            norm = [58650, 55500]
        elif self.y == 2025:
            norm = [60450, 57200]
        elif self.y == 2026:
            norm = [62300, 58950]
        else:
            self.logger.debug(f'y = {self.y}')
            raise ValueError('Ошибка в значении текущего года')

        #Сохранить в эксель накопительную сначала месяца
        
        dt = datetime.datetime.now().strftime('%H:%M %d.%m.%Y')
                
        d = {'Котировка':['Бензин-регуляр92', 'Дизель'], 'Средняя цена с начала месяца, руб./т':[self.abdt.iloc[0,0], self.abdt.iloc[0,1]], 'Норматив по НК РФ, руб./т':norm, 'Лимит отклонения цены, % от [2]':[0.1, 0.2], 'yest':[self.abdt_y.iloc[0,0], self.abdt_y.iloc[0,1]]}
        
        info = f'Средняя цена АБ92-регуляр {d["Средняя цена с начала месяца, руб./т"][0]} \nСредняя цена ДТ {d["Средняя цена с начала месяца, руб./т"][1]}'
        self.logger.info(info)
        
        dx = pd.DataFrame(d)
        
        dx['Верхняя граница лимита для получения демпфера, руб./т'] = dx['Норматив по НК РФ, руб./т'] * dx['Лимит отклонения цены, % от [2]'] + dx['Норматив по НК РФ, руб./т']
        dx['Отклонение от норматива, %'] = ((dx['Средняя цена с начала месяца, руб./т'] - dx['Норматив по НК РФ, руб./т'])/dx['Норматив по НК РФ, руб./т'] * 100).round(2)
        dx['Отклонение от верхней границы цены, руб./т'] = dx['Средняя цена с начала месяца, руб./т'] - dx['Верхняя граница лимита для получения демпфера, руб./т'] 
        dx['Изменение накопленной средней за месяц за предыдущий день, руб/т'] = dx['Средняя цена с начала месяца, руб./т'] - dx['yest']
        dx['Получение демпфера в текущем месяце'] = 'НЕТ' if (dx.iloc[0,7] >= 0) or (dx.iloc[1,7] >= 0) else 'ДА'
        dx = dx.drop('yest', axis=1)
        
        r = ['[0]','[1]','[2]','[3]','[4]=[1]*(1+[3])','[5]=[1]/[2]-1','[6]=[1]-[4]','[7]=[1]-([1] за предыдущий день)','[8]']
        c = dx.columns
        rn = pd.DataFrame(r).T.set_axis(c, axis=1)

        s0 = f'<p>Добрый день!<br>Направляю текущие средние Цаб_вр и Цдт_вр по итогам торгов {dt}</p><p><a href="https://blps-datalab/sense/app/b334752d-ee58-46db-8ff0-764b6ea10a3c/sheet/bb5a5146-03b7-43a1-8d8b-48de26d71c8e/state/analysis"> Посмотреть динамику на Дашборде </a></p>'
        s1 = pd.concat([rn, dx]).to_html(index=False)
        s2 = '<p>*Если хотя бы по одному из видов топлива значение по столбцу [6] > 0 (т.е. средняя накопленная цена с начала месяца больше норматива, увеличенного на лимит отклонения цены (т.е. результат столбца 4), то в столбце [8] для обоих топлив демпфера <b>не будет</b> (т.е. проставляется ответ <b>НЕТ</b>).<br>Чтобы демпфер был, необходимо, чтобы одновременно по обоим видам топлива (АБ и ДТ) значение в столбце [6] было <b>меньше 0</b>.</p>'
        
        #return s1

        return s0+s1+s2

    def create_fas_akciz(self) -> pd.DataFrame:

        dfs = []
        for i in self.data:
            m = pd.read_html(str(i), flavor='lxml')
            dfs.append(pd.concat(m, axis=1))
        
        y = datetime.datetime.now().year
        
        def add_vr():
            # [Цаб_вр, Цдт_вр] по годам
            # Указано к в НК в текущей редакции. Править тут руками в случае изменений в НК
            match y:
                case 2023:
                    ab, dt = 56900, 53850
                case 2024:
                    ab, dt = 58650, 55500
                case 2025:
                    ab, dt = 60450, 57200
                case 2026:
                    ab, dt = 62300, 58950
            return ab, dt
        
        def add_C():
            #Функция для добавления в выгрузку условных значений Цабвр_С и Цдтвр_С
            match y:
                case 2023:
                    ab = 68068
                    dt = 60775
                case 2024:
                    ab = 71472
                    dt = 63814
                case 2025:
                    ab = 75046
                    dt = 67005
                case 2026:
                    ab = 78798
                    dt = 70355
            return ab, dt
        
        def add_2021():
            #Функция для добавления в выгрузку условных значений Цабвр_2021 и Цдтвр_2021
            match y:
                case 2023:
                    ab = 62000
                    dt = 56000
                case 2024:
                    ab = 65000
                    dt = 58700
                case 2025:
                    ab = 68300
                    dt = 61700
                case 2026:
                    ab = 71700
                    dt = 64800
            return ab, dt
        
        d = dfs[-1].T
        d.columns = d.iloc[0]
        d = d.drop(0)
        
        d['Цабвр'], d['Цдтвр'] = add_vr()
        d['Цабвр_С'], d['Цдтвр_С'] = add_C()
        d['Цабвр_2021'], d['Цдтвр_2021'] = add_2021()
        
        #Кинв от года
        d['Кнв'] = 1.3 if y in range(2023, 2027, 1) else 1 
        
        self.fas = d
        return self
        
    def create_oil_monitoring(self, dt: str = None) -> pd.DataFrame:
        """
        Метод возвращает датафрейм с последними ценами Юралс и НСД в период мониторинга, $
        
        :param str dt: дата публикации цен (инстанс dt после вызова метода класса client.get_oilprice_monitoring), defaults to None
        :return pd.DataFrame: датафрейм с ценами для расчета ЭП (P) 
        """
        
        df = pd.read_html(self.data)
        df = df[0]
        
        df.iloc[0,0] = dt
        df[df.columns[1]] = df[df.columns[1]].str.extract(pat='(\d+,\d+)')

        self.monitoring = df 
        return self
    