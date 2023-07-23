from typing import List, AnyStr, Any
import json
from pydantic import BaseModel, Json, Field
#from src.app import ReestrRequest

'''
class OutputData(BaseModel):
            link: list = Field(alias='Ссылка на карточку лицензии')
            num: list = Field(alias='Государственный регистрационный номер')
            image: list = Field(alias='Наличие полного электронного образа')
            date: list = Field(alias='Дата')
            purpoise: list = Field(alias='Целевое назначение лицензии')
            tipe: list = Field(alias='Вид полезного ископаемого')
            name: list = Field(alias='Наименование участка недр, предоставленного в пользование по лицензии, кадастровый номер месторождения или проявления полезных ископаемых в ГКМ')
            state: list = Field(alias='Наименование субъекта Российской Федерации или иной территории, на которой расположен участок недр')
            coords: list = Field(alias='Географические координаты угловых точек участка недр, верхняя и нижняя границы участка недр')
            status: list = Field(alias='Статус участка недр')
            owner_full: list = Field(alias='Сведения о пользователе недр')
            agency: list = Field(alias='Наименование органа, выдавшего лицензию')
            doc_details: list = Field(alias='Реквизиты документа, на основании которого выдана лицензия на пользование недрами')
            changes: list = Field(alias='Сведения о внесении изменений и дополнений в лицензию на пользование недрами, сведения о наличии их электронных образов')
            forw_full: list = Field(alias='Сведения о переоформлении лицензии на пользование недрами')
            order: list = Field(alias='Реквизиты приказа о прекращении права пользования недрами, приостановлении или ограничении права пользования недрами')
            date_stop: list = Field(alias='Дата.1')
            stop_end_conditions: list = Field(alias='Срок и условия приостановления или ограничения права пользования недрами')
            date_end: list = Field(alias='Дата.2')
            prew_full: list = Field(alias='Сведения о реестровых записях в отношении ранее выданных лицензий на пользование соответствующим участком недр')
            link_alsn: list = Field(alias='Ссылка на АСЛН')
            #дропнуть
'''

class cols(BaseModel):
    col: Any

class values(BaseModel):
    val: Any

class data(BaseModel):
    cols: list[cols]
    values: list[values]

class result(BaseModel):
    data: dict(data)

with open('data/response.json', mode='r') as file:
    test_json = json.load(file)

#d = test_json['result']['data']['values']

x = result(**test_json)