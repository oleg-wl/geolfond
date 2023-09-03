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
        data = parser.get_data_from_reestr(filter)
        df = Parser.create_df(data)
        
        n = sum(x is None for x in df['forw_full']
            ), len(df["date"])
        click.echo("Данные загружены успешно. Всего лицензий: {:d}. Действующих лицензий: {:d}.".format(
                    n[1], n[0]
                )
            )
        Parser.save_df(df, filter)
        click.echo('ДАнные сохранены. Генерирую матрицу.')
        Parser.create_matrix(df)
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
    pass

@click.command()
def update():
    """
    Простая команда для обновления данных о нефтяных ЛУ и отправки на почту
    """
    click.echo("Погнали... {n}".format(n=art("rand")))
    run_code()
    click.echo("Успех {n}\n".format(n=art("rock on2")))


@click.group()
def cli():
    pass


cli.add_command(info)
cli.add_command(filters)
#cli.add_command(download)
#cli.add_command(matrix)
cli.add_command(update)


if __name__ == "__main__":
    cli()
