#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Запуск программы из консоли с выводом информации в консоль
"""
import click
import Parser

# Блок с функциями логики проекта

def oil_main():
    parser = Parser.multipl()
    data = parser.get_data_from_reestr(filter="oil")

    #: oil.xlsx
    tr = Parser.transformer(data=data)
    tr.create_df()

    # сохранить в файл свод датафреймов отдельным листом
    saver = Parser.saver(dfs=tr.rosnedra, sheets="oil")
    saver.save(name="reestr.xlsx")

def ndpi_main() -> None:
    #: Функция для скачивания только oil

    dfs = []  # Контейнер для датафреймов
    sheets = []  # Контейнер названий листов

    parser = Parser.multipl()
    tr = Parser.transformer()

    # Средний Юралс (P) и средний курс $ (Ц)
    curr = parser.get_currency("01.01.2023")
    pr = parser.get_oil_price(rng=7)
    tr.create_prices(curr=curr, pr=pr)
    dfs.append(tr.prices)
    sheets.append("prices")

    # Для Кабдт с ФАС
    fas_data = parser.get_fas_akciz()
    fas = Parser.transformer(data=fas_data)
    fas.create_fas_akciz()
    dfs.append(fas.fas)
    sheets.append("fas")

    #: Юралс в периоде мониторинга
    mon = parser.get_oilprice_monitoring()
    monitoring = Parser.transformer(data=mon.data)
    monitoring.create_oil_monitoring()
    dfs.append(monitoring.monitoring)
    sheets.append("monitoring")

    saver = Parser.saver(dfs=dfs, sheets=sheets)

    # сохранить в файл свод датафреймов отдельным листом
    saver.save(name='ndpi.xlsx')

def send(filename):
    # Отправка сообщения
    ms = Parser.sender()
    # Если в config.ini несколько адресатов, отправлять только на последний
    ms.smtp_to = [ms.smtp_to[-1]]

    ms.create_message(
        subj="Данные для дашборда",
        all=False,
        filename=[filename],
        htmlstr="Выгрузка данных для аналитики",
    )
    ms.send_message_f()


@click.command()
def info():
    """
    О программе
    """
    click.echo("2024 год. Загрузка данных для расчета НДПИ.")
    click.echo("email для связи geolfondapp@mail.ru")


@click.command()
def init():
    """
    Начальная конфигурация перед первым запуском. Создает папку для сохранения и конфигфайл из примера.
    """
    Parser._conf.basic_config()


@click.command()
def oil_reestr():
    """
    Выгрузить реестр лицензионных участков УВ и направить на почту
    """
    click.echo("Загрузка данных из реестра")
    oil_main()
    click.echo("Отправка данных")
    send('reestr.xlsx')


@click.command()
def ndpi():
    """
    Загрузить данные для расчета НДПИ и отправить таблицу на электронную почту. Данные почтового сервера и адресатов укажи в config.ini
    """
    click.echo("Загрузка данных для НДПИ")
    ndpi_main()
    click.echo("Отправка данных")
    send('ndpi.xlsx')
    
@click.command()
def download():
    """
    Только загрузить данные без отправки на электронную почту. Сохранить таблицы с данными в папке data
    """
    click.echo("Загрузка данных из реестра")
    #oil_main()
    click.echo("Загрузка данных для НДПИ")
    ndpi_main()
    
@click.command()
def reestr():
    """
    Скачать данные из реестра по разным фильтрам.
    """
    click.echo("Функционал будет добавлен")
    pass

@click.command()
def matrix():
    click.echo("Загрузка данных из реестра")
    parser = Parser.multipl()
    data = parser.get_data_from_reestr(filter="oil")

    tr = Parser.transformer(data=data)
    tr.create_df()
    tr.create_matrix()

    ms = Parser.sender()
    ms.smtp_to = [ms.smtp_to[-1]]

    ms.create_message(
        subj="Выгрузка лицензионных участков для ГБЗ",
        all=False,
        filename=["matrix.xlsx"],
        htmlstr="Исторические данные лицензионных участков по годам",
    )
    ms.send_message_f()

@click.group()
def cli():
    pass
cli.add_command(info)
cli.add_command(init)
cli.add_command(oil_reestr)
cli.add_command(matrix)
cli.add_command(ndpi)
cli.add_command(download)

if __name__ == "__main__":
    cli()
