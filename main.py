#!venv/bin/python
# -*- coding=utf-8 -*-

"""
Запуск программы из консоли с выводом информации в консоль
"""

import click
from requests import request
from art import tprint, art

from src.panda import ReestrData
from src.queries import lfilt


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
    for key, value in lfilt.items():
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

    # Проверка подключения
    rcode = request(method="GET", url="https://rfgf.ru/ReestrLic/")

    if int(rcode.status_code) == 200:
        try:
            # Получение данных
            click.echo("Погнали... {n}\n".format(n=art("rand")))
            click.echo("Загрузка данных...")
            data = ReestrData(str(filter))
            
            # Вывод в консоль
            n = sum(
                x is None
                for x in data.data[
                    "Сведения о переоформлении лицензии на пользование недрами"
                ]
            ), len(data.data["Дата"])
            click.echo(
                "Данные загружены успешно. Всего лицензий: {:d}. Действующих лицензий: {:d}.".format(
                    n[1], n[0]
                )
            )
            click.echo(f"Сохранение данных в {data.path}")
            data.save()
            click.echo("Успех {n}\n".format(n=art("rock on2")))

        except Exception as e:
            click.echo(f"Error: {e}")
    else:
        click.echo(
            "Что-то не так с подключением. Ошибка: {status}.".format(status=rcode)
        )


@click.command()
def matrix():
    """
    Скачать матрицу данных об лицензиях по годам, в разработке
    """
    pass


@click.group()
def cli():
    pass


cli.add_command(info)
cli.add_command(filters)
cli.add_command(download)
cli.add_command(matrix)


if __name__ == "__main__":
    cli()
