import logging
import re
import datetime
from io import StringIO
import certifi

from bs4 import BeautifulSoup as bs

from ..headers import headers_price as _hp
from ..headers import headers_price_duty as _hpd
from ..headers import url_smtb as _urlsmtb
from ..headers import url_fas as _urlfas

from .reestr import ReestrParser


class MultiplParser(ReestrParser):
    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger("multipl")
        self.now = datetime.datetime.now()

    def get_currency(
        self, start_date: str, end_date: str = None, today: bool = True
    ) -> dict:
        """Метод для получения среднемесячного курса доллара для расчета коэф Р в Кц НДПИ
        NOTE: Формат дат для подстановки в запрос - дд.мм.гггг

        Args:
            start_date (str): Начальная дата для подстановки в GET-запрос в формате дд.мм.гггг
            end_date (str): Конечная дата для подстановки в GET-запрос в формате дд.мм.ГГГ (Dafault = None)
            taday (bool): Default True выбор текущей даты (сегодня). Если False то укажи end_date

        Returns:
            dict: python словарь. Ключ Date - значение торгового дня; ключ Rate - значение средней ставки ЦБ РФ в этот день
        """
        if today:
            end_date = datetime.datetime.strftime(datetime.datetime.now(), "%d.%m.%Y")
        elif end_date == None:
            self.logger.error("Если today=False, укажи end_date")
            raise ValueError

        pat = r"\b(0[1-9]|[1-2]\d|3[0-1])\.(0[1-9]|1[0-2])\.\d{4}\b"
        if not re.fullmatch(pat, start_date) or not re.fullmatch(pat, end_date):
            self.logger.error("Неверный формат даты. Используй дд.мм.гггг")
            raise ValueError(f"{start_date}{end_date}")
        else:
            url = f"https://cbr.ru/currency_base/dynamics/?UniDbQuery.Posted=True&UniDbQuery.so=1&UniDbQuery.mode=1&UniDbQuery.date_req1=&UniDbQuery.date_req2=&UniDbQuery.VAL_NM_RQ=R01235&UniDbQuery.From={start_date}&UniDbQuery.To={end_date}"
            r = self.session.get(url=url)
            self.logger.info("Загружаю средний курс ЦБ РФ")

            raw = bs(r.text, "html.parser")
            table_tag = raw.table

            rows = [i for i in table_tag.stripped_strings][5:]
            data = {
                "Dates": [v for n, v in enumerate(rows) if n % 3 == 0],
                "Rate": [rows[n + 2] for n, v in enumerate(rows) if n % 3 == 0],
            }

            return data

    def get_oil_price(self, rng: int = 7) -> dict[datetime.datetime, str]:
        """
        Метод для парсинга цен на нефть с сайта Минэкономразвития. Публикуемые котировки Argus для расчета коэфициента Кц.

        :return dict: словаь с ценами
        """

        url1: str = "https://economy.gov.ru/material/departments/d12/konyunktura_mirovyh_tovarnyh_rynkov/?type=&page="
        url2: str = "https://economy.gov.ru"
        headers: dict = _hp

        dates: list = []
        prices: list = []

        pat_dt = re.compile(r"(?P<date>\w* \d{4})")
        pat_price = re.compile(r"(?P<usd>\d{1,3},\d{1,2})")
        self.logger.info("Загружаю котировки Argus")
        for counts in range(1, rng):
            resp = self.session.get(url=url1 + str(counts), headers=headers)

            data = bs(resp.text, "html.parser")

            for i in data.find_all("a", attrs={"title": re.compile("Юралс")}):
                link = i["href"]

                raw = self.session.get(url=url2 + link, headers=headers)
                html_data = bs(raw.text, "html.parser")

                txt = html_data.get_text(strip=True)

                dt = re.findall(pat_dt, txt)[1]
                dates.append(dt)

                pr = re.findall(pat_price, txt)[0]
                prices.append(pr)

        #: Переименовать месяц для объекта datetime
        dates_fixed = []
        for item in dates:
            for old, new in [
                ("январь", "01"),
                ("февраль", "02"),
                ("март", "03"),
                ("апрель", "04"),
                ("май", "05"),
                ("июнь", "06"),
                ("июль", "07"),
                ("август", "08"),
                ("сентябрь", "09"),
                ("октябрь", "10"),
                ("ноябрь", "11"),
                ("декабрь", "12"),
            ]:
                if old in item:
                    d = datetime.datetime.strptime(item.replace(old, new), "%m %Y")
                    dates_fixed.append(d)

        return {"Dates": dates_fixed, "Price": prices}

    def get_abdt_index(self) -> dict:
        #: Требуемые индексы захардкодил.
        ind = ["reg", "dtl", "dtm", "dtz"]
        d = {}
        try:
            self.logger.info("Загружаю цены с СПБ биржи")
            for index in ind:
                r = self.session.get(_urlsmtb.format(index=index))
                d[index] = StringIO(r.text)

            return d
        except Exception as e:
            self.logger.error("Ошибка загрузки цен с биржи")
            self.logger.debug(e)
            raise

    def get_oilprice_monitoring(self):
        """Метод для получения данных Р в ЭП (Средняя цена юралс в период мониторинга)
        Возвращает два инстанса класса
        date - str - html-таблица с данными
        dt - str - дата публикации данных на сайте
        Returns:
            self
        """

        # Основная ссылка для запроса
        try:
            self.logger.info("Загружаю цены Юралс и Брент в мониторинге")
            
            #Получаем ссылки на последние публикации по нефти
            u = "https://www.economy.gov.ru/material/directions/vneshneekonomicheskaya_deyatelnost/tamozhenno_tarifnoe_regulirovanie/submaterials/?type=&page="
            url1 = "https://www.economy.gov.ru/material/directions/vneshneekonomicheskaya_deyatelnost/tamozhenno_tarifnoe_regulirovanie/"
            #resp = self.session.get(url=url1, headers=_hpd).text

            links = []
            for p in range(1, 3):
                self.logger.debug('запрос к минек  %s' %p)
                resp = self.session.get(u+str(p), headers=_hpd).text    
                page = bs(resp, "html.parser")
                l: list = page.find_all(
                "a", attrs={"title": re.compile("вывозных таможенных пошлин.* на нефть")}
                )
                for i in l:
                    links.append(i)

            #Переходим по каждой ссылке
            patt = r"(\d{2} \w+ \d{4})" # паттерн даты публикации       

            self.data: dict = {}
            for link in links:
                date = re.search(patt, link['title']).group()
                for old, new in [
                ("января", "01"),
                ("февраля", "02"),
                ("марта", "03"),
                ("апреля", "04"),
                ("мая", "05"),
                ("июня", "06"),
                ("июля", "07"),
                ("августа", "08"),
                ("сентября", "09"),
                ("октября", "10"),
                ("ноября", "11"),
                ("декабря", "12")]:
                    if old in date:
                        d = datetime.datetime.strptime(date.replace(old, new), "%d %m %Y")
                        self.logger.debug('запрос к дате эп %s' %d)
                        url2 = link.get("href").rsplit(sep="/")[-1]
            
                        resp = self.session.get(url=url1 + url2, headers=_hpd).text
                        page = bs(resp, "html.parser")

                        table: str = page.find("table").prettify()
                        self.data[d] = table
        except:
            self.logger.error(f"Ошибка при парсинге сайта минэка")
            raise
        return self

    def get_fas_akciz(self) -> str:
        self.logger.info("Загружаю цены ФАС")

        r = self.session.get(url=_urlfas, verify=certifi.where())
        #r = self.session.get(url=_urlfas, verify=False)
        d = bs(r.text, "html.parser")

        return [x.prettify() for x in d.find_all("table")]
