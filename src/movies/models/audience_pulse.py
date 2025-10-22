import pandas as pd

from src.movies.models.data_provider import DataProvider
from src.movies.schema.movies_merge_schema import schema, audience_pulse_mapping


class AudiencePulse(DataProvider):
    def __init__(self, file_path):
        super().__init__(file_path)
        self.df = pd.read_json(self.filePath)

    def parse_schema(self):
        """

        :return:
        """
        # Use imported mapping
        self.df = self.df.rename(columns=audience_pulse_mapping)

        # Apply schema types
        self.df = self.df.astype(schema)
