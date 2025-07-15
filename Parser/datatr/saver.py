from email import message
import logging
from typing import TypeVar, List
import pandas as pd
import os
from ..base_config import BasicConfig

logger = logging.getLogger("saver")


class DataSaver(BasicConfig):
    T = TypeVar("T", List[pd.DataFrame], pd.DataFrame, str)

    def __init__(self, dfs: T = None, sheets: T = None, concat: bool = False) -> None:
        """
        Сохранение датафрейма в формате Excel

        :param T dfs: датафрейм или лист датафреймов джля сохранения, defaults to None
        :param T sheets: название листа или лист с названиями, defaults to None
        :param bool concat: _description_, defaults to False
        :raises logger.exception: _description_
        """
        super().__init__()

        if isinstance(dfs, pd.DataFrame) and isinstance(sheets, str):
            self.dfs = [dfs]
            self.sheets = [sheets]

        elif isinstance(dfs, list) and isinstance(sheets, list):
            self.dfs = dfs
            self.sheets = sheets
            if len(self.dfs) != len(self.sheets):
                logger.error(
                    "Количетсво датафреймов не равно количеству листов"
                )
        else:
            logger.error("dfs must be pd.Dataframe or list")
            

        self.concat: bool = concat

        if self.concat:
            self.dfs = list(pd.concat(self.dfs))

    def save(self, name: str = "main.xlsx"):
        file = os.path.join(self.path, name)

        with pd.ExcelWriter(file) as writer:
            c = 0
            for df, sheet in zip(self.dfs, self.sheets):
                c += 1
                df.to_excel(writer, sheet_name=sheet, freeze_panes=(1, 0), na_rep="", engine='openpyxl')
        logger.info(f"Сохранено {c} листов в {file}")
