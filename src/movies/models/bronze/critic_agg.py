import pandas as pd

from src.movies.models.bronze.data_provider import DataProvider


class CriticAgg(DataProvider):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.df = pd.read_csv(self.filePath)
