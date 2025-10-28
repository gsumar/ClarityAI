
from src.movies.models.silver.data_provider import DataProvider


class AudiencePulse(DataProvider):
    def __init__(self, df, version='v1'):
        super().__init__(df)
        self.version = version
        self.df = self.parse_schema()

    def parse_schema(self):
        """
        Transform Bronze layer data to Silver layer using Schema Registry

        :return: Transformed DataFrame
        """
        return self.registry.transform_dataframe('silver/audience_pulse', self.version, self.df)
