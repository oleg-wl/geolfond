#!venv/bin/python
# -*- coding=utf-8 -*-

"""
Запуск программы из консоли с выводом информации в консоль
"""

import click
import requests
from art import tprint, art

import Parser
from geolfond import run_code

@click.command()
def info():
    """
    О программе
    """
    tprint("geolfond")
    click.echo("2023 год. Загрузка открытых данных РФРГ.")
    click.echo('email для связи geolfondapp@mail.ru')


@click.command()
def filters():
    """
    Показать список доступных фильтров для downloads
    """
    filter_list = Parser._filter()
    for key, value in filter_list.items():
        click.echo(f"-  {key}    для фильтра: {value[0:24]}...")



@click.command()
@click.option(
    "-f",
    "--filter",
    default="oil",
    type=str,
    show_default=True,
    help="Укажи значение фильтра из команды filter",
)
def download(filter: str):
    """
    Скачать данные c реестра
    """
    click.echo("Погнали... {n}".format(n=art("rand")))
    click.echo("Загрузка данных...")

    try:
        parser = Parser.client()
        tr = Parser.transformer()
        
        data = parser.get_data_from_reestr(filter)
        df = tr.create_df(data)
        
        n = sum(x is None for x in df['forw_full']
            ), len(df["date"])
        click.echo("Данные загружены успешно. Всего лицензий: {:d}. Действующих лицензий: {:d}.".format(
                    n[1], n[0]
                )
            )
        tr.save_df(df, filter)
        click.echo(f'Данные сохранены в {tr.path}')
        click.echo("Успех {n}\n".format(n=art("rock on2")))

    except requests.exceptions.Timeout as e:
        click.echo(f"Timeout error: {e}. Возможно надо подключиться через прокси. Сервер доступен только из РФ.")
        
    except requests.exceptions.RequestException as e:
        click.echo(f"Connection error: {e}")
    except Exception as e:
        click.echo(f"Some problem  {e}. Sorry: {art('umadbro')}")
        


@click.command()
@click.option(
    "-f",
    "--filter",
    default="oil",
    type=str,
    show_default=True,
    help="Укажи значение фильтра из команды filter",
)
def matrix(filter: str):
    """
    Скачать матрицу данных об лицензиях по годам, в разработке
    В разработке
    """
    try:
        parser = Parser.client()
        tr = Parser.transformer()
    
        data = parser.get_data_from_reestr(filter)
        df = tr.create_df(data)
        tr.create_matrix(df)
        click.echo(f'Матрица сохранена в {tr.path}')
        click.echo("Успех {n}\n".format(n=art("rock on2")))
    
    except Exception as e:
        raise e
        

@click.command()
def update():
    """
    Простая команда для обновления данных о нефтяных ЛУ и отправки на почту
    """
    click.echo("Погнали... {n}".format(n=art("rand")))
    run_code()
    click.echo("Успех {n}\n".format(n=art("rock on2")))

@click.command()
@click.option('--type', type=str, help='json для сохранения в виде json; xlsx для сохранения в виде экселя')
def get_raw_data(type):
    """Выгрузить данные в формате json 

    Args:
        type (_type_): _description_
    """
    if isinstance(type, str):
        if type == 'json':
            pass
        elif type == 'xlsx':
            pass 
    pass

@click.command()
def get_prices():
    """
    Получить данные среднего курса ЦБ РФ и Argus
    """
    parser = Parser.client()
    tr = Parser.transformer()
    
    curr = parser.get_currency(start_date='01.01.2021')
    pr = parser.get_oil_price(rng=7)
    
    tr.create_prices(curr=curr, pr=pr)
    

@click.group()
def cli():
    pass


cli.add_command(info)
cli.add_command(filters)
cli.add_command(download)
cli.add_command(matrix)
cli.add_command(update)
cli.add_command(get_prices)
cli.add_command(get_raw_data)



if __name__ == "__main__":
    cli()
