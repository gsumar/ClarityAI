
from src.movies.models.bronze.data_provider import DataProvider
from src.movies.schema.movies_merge_schema import schema


class CriticAgg(DataProvider):
    def __init__(self, df):
        self.df = df
        super().__init__(df)
        self.df = self.parse_schema()

    def parse_schema(self):
        """

        :return:
        """
        return self.df.astype(schema)
