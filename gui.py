from tkinter import *
from tkinter import filedialog as fd
from tkinter import messagebox as mb

from app import ReestrRequest

import sys



import os

"""
Создание GUI для выгрузки из реестра
"""

root = Tk()
root.title('ROSNEDRA')
#root.geometry('400x200')

#fpath = os.path.join(os.getcwd(), 'Реестр')
fpath = os.path.expanduser('~/Dropbox/Проекты/Роснедра/Реестр/')

def openExplorer():
    #Функция для кнопки открытия проводника с результатами выгрузки. путь: рабочая папка+Реестр
    ostype = sys.platform 

    if 'win' in ostype:
        command = 'explorer.exe ' + fpath
    elif 'linux' in ostype:
        command = '/usr/bin/caja ' + fpath #Запуск штатного менеджера в Линукс Минт
    os.system(command)

#TODO: Функция для кнопки для запуска выгрузки из реестра
def parseReestr():
    pass

#TODO: Функция для кнопки для выбора и открытия двух файлов для Compairera
def openFiles() -> list:

    file1 = fd.askopenfilename()
    file2 = fd.askopenfilename()
    pass

#TODO: Функция для кнопки Compairer'a:

def Compairer():
    pass

    
#TODO: Функция для кнопки для выхода
def exit():
    pass


greet = Label(root, text='Привет!\nЯ скачаю последнюю версию реестра из Государственного реестра участков недр и лицензий\nИзвлеку Координаты участков, ИНН и Наименование недропользователя и сохраню в формате EXCEL.', justify=LEFT)

button_query = Button(root, text='Выгрузить данные', command=parseReestr)
button_compare = Button(root, text='Анализ данных', command=Compairer)
button_explorer = Button(root, text='Открыть папку с данными', command=openExplorer)
button_exit = Button(root, text='Выход', command=exit)

greet.grid(row = 0, columnspan=8)
button_query.grid(row=1, column=0, sticky=NW)
button_compare.grid(row=1, column=1)
button_explorer.grid(row=1, column=3)
button_exit.grid(row=1, column=4)

root.mainloop()
