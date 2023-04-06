#!venv/bin/python
# -*- coding=utf-8 -*-

"""
Запуск программы из консоли с выводом информации в консоль
"""
import os
import click
from requests import request
from art import tprint, art

from app import ReestrRequest, dpath
from src.panda import save_in_excel
from src.queries import lfilt



@click.command()
def info():
    '''
    О программе
    '''
    tprint('geolfond')
    click.echo('2023 год. Загрузка открытых даных рфгф.')

@click.command()
def filters():
    '''
    Показать список доступных фильтров для downloads
    '''
    for key, value in lfilt.items():
        click.echo (f'-  {key}    для фильтра: {value[0:24]}...')
    

@click.command()
@click.option('-f', '--filt', default='oil', type=str, show_default=True, help='Укажи значение фильтра из команды filter')
def download(filt: str):
    '''
    Скачать данные c реестра
    '''

    #Проверка подключения
    rcode = request(method='GET', url='https://rfgf.ru/ReestrLic/')

    if int(rcode.status_code) == 200:
        
        try:
    
            # Получение данных
            click.echo('Погнали... {n}\n'.format(n = art('rand')))
            click.echo('Загрузка данных...')
            requested_data = ReestrRequest(lfilt[filt])
            data = requested_data.create_df()

            # Вывод в консоль 
            n = sum(x is None for x in requested_data.nested_data['Сведения о переоформлении лицензии на пользование недрами']), len(requested_data.nested_data['Дата'])
            click.echo('Данные загружены успешно. Количество записей: {:d}. Действующих лицензий: {:d}.'.format(n[1], n[0]))
            click.echo('Обработка данных...')

            # Проверка целостности выгружаемых данных
            click.echo('Сохранение данных...')
            
            # Сохренение в excel
            desk = os.path.join(dpath, f'reestr_{filt}.xlsx')
            save_in_excel(path=desk, dataframe_to_save=data, name_for_sheet=filt)
            click.echo('Выгрузка завершена. Данные сохранены в {}'.format(desk))
 
        except Exception as e:
            click.echo(f"Error: {e}")    
    else:
        click.echo("Что-то не так с подключением. Ошибка: {status}.".format(status=rcode))

@click.command()
def matrix():
    '''
    Скачать матрицу данных об лицензиях по годам, в разработке
    '''
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
