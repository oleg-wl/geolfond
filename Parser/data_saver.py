import pandas as pd
import os
from .data_transformer import DataTransformer


class DataSaver(DataTransformer):
    def __init__(
        self,
        data: [str | list | dict] = None,
        dt=None,
        dfs: list = None,
        sheets: list = None,
    ) -> None:
        super().__init__(data)
        self.dfs = dfs
        self.sheets = sheets

    def save(self, name: str = "main.xlsx"):
        file = os.path.join(self.path, name)

        with pd.ExcelWriter(file) as writer:
            c = 0
            for df, sheet in zip(self.dfs, self.sheets):
                c += 1
                df.to_excel(writer, sheet_name=sheet, freeze_panes=(1, 0), na_rep="")
        self.logger.info(f"Сохранено {c} листов в {file}")
