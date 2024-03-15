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

        if type(dfs, pd.DataFrame) and type(sheets, str):
            self.dfs = list(dfs)
            self.sheets = list(sheets)

        elif type(dfs, list):
            self.dfs = dfs
            self.sheets = sheets
            if len(self.dfs) != len(self.sheets):
                raise logger.exception(
                    "Количетсво датафреймов не равно количеству листов"
                )
        else:
            raise logger.exception("dfs must be pd.Dataframe or list")

        self.concat: bool = concat

        if self.concat:
            self.dfs = list(pd.concat(self.dfs))

    def save(self, name: str = "main.xlsx"):
        file = os.path.join(self.path, name)

        with pd.ExcelWriter(file) as writer:
            c = 0
            for df, sheet in zip(self.dfs, self.sheets):
                c += 1
                df.to_excel(writer, sheet_name=sheet, freeze_panes=(1, 0), na_rep="")
        logger.info(f"Сохранено {c} листов в {file}")
