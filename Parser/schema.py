#: 

def create_table(table_name: str, columns: str) -> str:
    return f'DROP TABLE IF EXISTS {table_name};', f'CREATE TABLE {table_name}(id INTEGER NOT NULL PRIMARY KEY, {columns});'

def insert_into(table_name: str, rows: dict) -> str:
    cols = ','.join([f'{key}' for key in rows.keys()])
    vals = ','.join([f'{v}'for v in rows.values()])

    return rf'INSERT INTO {table_name} ({cols}) VALUES {vals}'
    