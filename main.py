#!venv/bin/python
# -*- coding=utf-8 -*-

"""
Запуск программы из консоли с выводом информации в консоль
"""

import click
import requests
from art import tprint, art

from src import panda, queries


@click.command()
def info():
    """
    О программе
    """
    tprint("geolfond")
    click.echo("2023 год. Загрузка открытых даных РФРГ.")


@click.command()
def filters():
    """
    Показать список доступных фильтров для downloads
    """
    for key, value in queries.lfilt.items():
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
        data = panda.ReestrData()
        data.create_df(filter)
        n = sum(x is None for x in data.data['forw_full']
            ), len(data.data["date"])
        click.echo("Данные загружены успешно. Всего лицензий: {:d}. Действующих лицензий: {:d}.".format(
                    n[1], n[0]
                )
            )
        click.echo(f"Сохранение данных в {data.path}")
        data.save()
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
    """
    try:
        click.echo("Погнали... {n}".format(n=art("rand")))
        data = panda.ReestrMatrix()
        data.create_matrix(filter)
        click.echo("Успех {n}\n".format(n=art("rock on2")))
        click.echo(f"Сохранение данных в {data.path}")

    except requests.exceptions.Timeout as e:
        click.echo(f"Timeout error: {e}. Возможно надо подключиться через прокси. Сервер доступен только из РФ.")
        
    except requests.exceptions.RequestException as e:
        click.echo(f"Connection error: {e}")
    except Exception as e:
        click.echo(f"Some problem  {e}. Sorry: {art('umadbro')}")

@click.group()
def cli():
    pass


cli.add_command(info)
cli.add_command(filters)
cli.add_command(download)
cli.add_command(matrix)


if __name__ == "__main__":
    cli()
