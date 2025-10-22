import pandas as pd

from src.movies.models.data_provider import DataProvider
from src.movies.schema.movies_merge_schema import schema


class CriticAgg(DataProvider):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.df = pd.read_csv(self.filePath)

    def parse_schema(self):
        """

        :return:
        """
        self.df.astype(schema)
