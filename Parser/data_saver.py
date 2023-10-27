import logging
import pandas as pd
import os
from .base_config import BasicConfig

logger = logging.getLogger('saver')

class DataSaver(BasicConfig):

    def __init__(
        self,
        dfs: list[pd.DataFrame],
        sheets: list[str],
        concat: bool = False
    ) -> None:
        """Класс для сохранения данных (датафреймов) в эксель файл
        Количетсво датафреймов должно быть равно количеству названий
        
        Args:
            dfs (list[pd.DataFrame]): Лист с датафреймами
            sheets (list[str]): Список с названиями листов для датафреймов в эксель файле
            concat (bool, optional): True если надо собрать список в один pd.concat. Defaults to False.
        """
        super().__init__()
        self.dfs = dfs
        self.sheets: list = sheets
        self.concat: bool = concat
        
        if self.concat:
            self.dfs: list(pd.concat(self.dfs))

    def save(self, name: str = "main.xlsx"):
        file = os.path.join(self.path, name)
        
        if len(self.dfs) == len(self.sheets):  
            with pd.ExcelWriter(file) as writer:
                c = 0
                for df, sheet in zip(self.dfs, self.sheets):
                    c += 1
                    df.to_excel(writer, sheet_name=sheet, freeze_panes=(1, 0), na_rep="")
            logger.info(f"Сохранено {c} листов в {file}")
        
        else:
            logger.error('Количество названий листов не равно количеству датафреймов')
