# geolfond
Получение данных о лицензиях на пользование недрами

## Функционал скрипта
Скрипт загружает данные из реестра лицензий на право пользование недрами <br>
Обрабатывает данные и сохраняет в таблицу Excel<br>
Сохраненную таблицу направляет на электронную почту<br>
В случае ошибок прилетит письмо с логами <br>

## New in version 2:
* Поддержка прокси (SOCKS5)
* Логирование ошибок

Извлекаются отдельные столбцы для ИНН, Наименование недропользователя, самой северной координаты (применимость льгот по НДД)

Добавляются столбцы: 
* prev_owner - название предыдущего недропользователя
* prev_lic - номер предыдущей лицензии для данной записи
* forw_lic - номер следующей лицензии для данной записи
* Last[True]: запись лицензии последняя по данному ЛУ
* Last[False]: запись лицензии не последняя (историеская) по данному ЛУ
* N - Самая сверная координата северной шиторы лицензионного учатка для определения применимости льготы по НДД для нефтегазовых провинций севернее 70-го градуса северной широт
* E - Соответствующая ей координата восточной долготы

## Установка
```
git clone https://github.com/oleg-wl/geolfond/
pip install -r requirements.txt
cat Parser/example.config.ini > Parser/config.ini
```
<br>


В процессе доработки CLI интерфейс на базе click <br>
Информация об основных командах
```
python main.py --help
```
filters - выводит список фильтров
update - скачать данные по УВС

Можно применить фильтр (в процессе доработки)
```
python main.py filter // показать код фильтра для вида ПИ лицензии
python main.py download // Запрос по фильтру кода ПИ лицензии (default 'oil')
python main.py download -f water //Запрос по фильтру кода ПИ лицензии "Подземные воды"
```

Быстро скачать или добавить в crontab 
```
python geolfond.py
``` 

Есть вопросы, пиши: [geolfondapp@mail.ru](mailto:geolfondapp@mail.ru?subject=Привет! Есть вопрос по парсеру)

