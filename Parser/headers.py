#: Переменные для создания фильтров и запроса к API


def filter(filter: str = None): 
    #Функция для быстрого выбора фильтра

    # Список фильтров
    lfilt: dict[str, str] = {
        "oil": "Н - Углеводородное сырье",
        "water": "В - Подземные воды (за исключением подземных минеральных вод)",
        "drag_met": "Б - Драгоценные металлы (золото, серебро, платина и металлы платиновой группы)",
        "undergr_radioact": "З - Подземное пространство, используемое для строительства и эксплуатации подземных сооружений для захоронения радиоактивных отходов (пунктов захоронения), отходов производства и потребления I - V классов опасности (объектов захоронения отходов)",
        "drag_stones": "К - Драгоценные камни (природные алмазы, изумруды, рубины, сапфиры, александриты)",
        "mineral_water": "М - Подземные минеральные воды, лечебные грязи, специфические минеральные ресурсы (рапа лиманов и озер, сапропель и другие)",
        "undergd_sci_obj": "О - Подземное пространство, используемое для образования особо охраняемых геологических объектов, имеющих научное, культурное, эстетическое, санитарно-оздоровительное и иное значение (научные и учебные полигоны, геологические заповедники, заказники, памятники природы, пещеры и другие подземные полости), сбора минералогических, палеонтологических и других геологических коллекционных материалов",
        "undergr": "П - Подземное пространство, используемое для строительства и эксплуатации подземных сооружений (за исключением подземных сооружений для захоронения радиоактивных отходов (пунктов захоронения), отходов производства и потребления I - V классов опасности (объектов захоронения отходов), для строительства и эксплуатации хранилищ углеводородного сырья, размещения в пластах горных пород попутных вод и вод, использованных пользователями недр для собственных производственных и технологических нужд при разведке и добыче углеводородного сырья, размещения в пластах горных пород вод, образующихся у пользователей недр, осуществляющих разведку и добычу, а также первичную переработку калийных и магниевых солей",
        "solid": "Т - Твердые полезные ископаемые",
    }
    if filter is None:
        return lfilt
    else:
        try:
            return filter, lfilt[filter]
        except KeyError:
            raise KeyError(f"Ошибка! Неверное значение фильтра для выгрузки {filter}") 
            

#: Название столбцов для pandas
#!: При изменении значений (=изменении столбцов) измени названия столбцов и пересоздай базу данных или где там у тебя данные хранятся
cols: dict[str, str] = {
    "Государственный регистрационный номер": "num",
    "Дата": "date",
    "Вид полезного ископаемого": "type",
    "Наименование субъекта Российской Федерации или иной территории, на которой расположен участок недр": "state",
    "Наименование участка недр, предоставленного в пользование по лицензии, кадастровый номер месторождения или проявления полезных ископаемых в ГКМ": "name",
    "Сведения о пользователе недр": "owner_full",
    "Географические координаты угловых точек участка недр, верхняя и нижняя границы участка недр": "coords",
    "Сведения о переоформлении лицензии на пользование недрами": "forw_full",
    "Сведения о реестровых записях в отношении ранее выданных лицензий на пользование соответствующим участком недр": "prew_full",
    "Реквизиты документа, на основании которого выдана лицензия на пользование недрами": "doc_details",
    "Наименование органа, выдавшего лицензию": "agency",
    "Статус участка недр": "status",
    "Целевое назначение лицензии": "purpose",
    "Наличие полного электронного образа": "image",
    "Ссылка на карточку лицензии": "link",
    "Сведения о внесении изменений и дополнений в лицензию на пользование недрами, сведения о наличии их электронных образов": "changes",
    "Реквизиты приказа о прекращении права пользования недрами, приостановлении или ограничении права пользования недрами": "ordered",
    "Дата.1": "date_stop",
    "Дата.2": "date_end",
    "Срок и условия приостановления или ограничения права пользования недрами": "stop_end_conditions",
    "Ссылка на АСЛН": "link_alsn",
}

# Сконвертированные переменные для запроса
url: str = "https://bi.rfgf.ru/corelogic/api/query"

headers: dict[str, str] = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0",
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3",
    # 'Accept-Encoding': 'gzip, deflate, br',
    # Already added when you pass json=
    # 'Content-Type': 'application/json',
    "Authorization": "Bearer NoAuth",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://bi.rfgf.ru",
    "Connection": "keep-alive",
    "Referer": "https://bi.rfgf.ru/viewer/public?dashboardGuid=ae176f70a6df4e81ba10247f44fb1191&showNav=false&fit=false",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    # Requests doesn't support trailers
    # 'TE': 'trailers',
}

json_data: dict = {
    "QueryType": "GetRawOlapData+Query",
    "RawOlapSettings": {
        "databaseId": "Main",
        "measureGroup": {
            "columns": [
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "c64a19c50c394605981e3ebe87918ae1",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "rcart_link",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "7c57c1b9e5b74322b5976eff809e3c20",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "lc_number_full",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "b146527321b840668975b16507194d33",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "has_doc",
                },
                {
                    "type": "RawOlapDimensionRoleAttributeColumnDto",
                    "kind": "RawOlapDimensionRoleAttributeColumnDto",
                    "guid": "3766824573874611bc87322d41091f2f",
                    "dimensionRoleId": {
                        "type": "DimensionRoleIdDto",
                        "kind": "DimensionRoleIdDto",
                        "value": "date_reg_start",
                    },
                    "attributeId": "DATE",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "dd98c164c5234359b58909f9b96f10da",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "purpose",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "0c54fe57b031469686122724a10fca61",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "lc_cat_type_abr_with_pi",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "7514ae69824d42fb999a6630fc6b902d",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "area_nedr_name",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "f5cd97e6be364c82a48e8bad88682e32",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "region_name_sf",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "15dfc1c77ad34713acd832b0b28cba33",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "koord",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "89880068c9514a94b74dd1ac6326c4ee",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "stat_uch",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "55928c0365254e989776b22c2ad91133",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "company_name",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "30e8b6fcda754620b37a8808eaad9822",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "organ",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "1565bb98bd09479dbb14db2a63d04ad6",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "doc_lic",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "8b0d834417a14859940ba47a37420323",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "doc_dopoln",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "86006ff857ee48c0b94c27a5c82743e4",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "lc_number_full_new",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "24644a616cba4dcba8369c584ba1cfea",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "annul_doc",
                },
                {
                    "type": "RawOlapDimensionRoleAttributeColumnDto",
                    "kind": "RawOlapDimensionRoleAttributeColumnDto",
                    "guid": "17bd7207b3b14433803e97480f5dc2a4",
                    "dimensionRoleId": {
                        "type": "DimensionRoleIdDto",
                        "kind": "DimensionRoleIdDto",
                        "value": "annul_date",
                    },
                    "attributeId": "DATE",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "930bb47ba3b242b2b91e81d457692a54",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "pause_dates",
                },
                {
                    "type": "RawOlapDimensionRoleAttributeColumnDto",
                    "kind": "RawOlapDimensionRoleAttributeColumnDto",
                    "guid": "a46c3241eeae4c5e9d81d71f84c03eab",
                    "dimensionRoleId": {
                        "type": "DimensionRoleIdDto",
                        "kind": "DimensionRoleIdDto",
                        "value": "date_reg_end",
                    },
                    "attributeId": "DATE",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "d02af800021e4f559a1120bae16e692f",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "num_rann_lic",
                },
                {
                    "type": "RawOlapDimensionAttributeColumnDto",
                    "kind": "RawOlapDimensionAttributeColumnDto",
                    "guid": "1fb7dfc7907545899a8bf78dc7c655b2",
                    "dimensionId": {
                        "type": "DimensionIdDto",
                        "kind": "DimensionIdDto",
                        "value": "R_litsenziya",
                    },
                    "attributeId": "asln_link",
                },
            ],
            "filters": [
                [
                    {
                        "selectedFilterValues": [
                            "Н - Углеводородное сырье",
                        ],
                        "attributeId": "S_rasshifrovkoi",
                        "dimensionOrDimensionRoleId": {
                            "type": "DimensionIdDto",
                            "kind": "DimensionIdDto",
                            "value": "R_litsenziya",
                        },
                        "useExcluding": False,
                    },
                ],
            ],
            "times": [],
            "id": "R_Reestr_litsenzii",
        },
        "lazyLoadOptions": {
            "columnSorts": [
                {
                    "columnindex": 3,
                    "order": 1,
                },
            ],
            "offset": 0,
            "limit": 20,
        },
    },
    "CalculationQueries": [],
    "AdditionalLogs": {
        "widgetGuid": "245d52ef1fdc474a8795fc37a8d03d02",
        "dashboardGuid": "ae176f70a6df4e81ba10247f44fb1191",
        "sheetGuid": "23760aa1c3844e9686946dfac2d0e8a0",
    },
    "WidgetGuid": "245d52ef1fdc474a8795fc37a8d03d02",
}

#: Запросы к минэк. Цены для P и P в ЭП
#: Средняя цена Юралс
headers_price = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/117.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'Referer': 'https://economy.gov.ru/material/departments/d12/konyunktura_mirovyh_tovarnyh_rynkov/',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Sec-GPC': '1',
}

#: Заголовки для запроса Цены на нефть в период мониторинга (Р в ЭП)
headers_price_duty = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/118.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Sec-GPC': '1',
}

#: GET запрос к сайту биржи СПБ по Цаб_вр и Цдт_вр
url_smtb = 'https://spimex.com/indexes/service_functions_oil.php?request=graph&code={index}&index=eti&market=P&litre=0&subcode=evr'

#: GET запрос к ФАС
url_fas = 'https://fas.gov.ru/pages/pokazateli-dla-vycheta-akciza'