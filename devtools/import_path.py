#import functions for Jupyter notebooks
import os
import sys

def import_from_linux():
    return os.path.normpath('~/reestr_oil.xlsx')

def import_from_win():
    return os.path.join(os.path.join(os.environ['USERPROFILE']), 'Desktop', 'reestr_oil.xlsx')

def imp_path():
    ostype = sys.platform
    if 'win' in ostype:
        path = import_from_win() 
    else:
        path = import_from_linux()
    return path