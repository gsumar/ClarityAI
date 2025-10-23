
from src.movies.models.bronze.data_provider import DataProvider
from src.movies.schema.movies_merge_schema import schema, audience_pulse_mapping


class AudiencePulse(DataProvider):
    def __init__(self, df):
        self.df = df
        super().__init__(df)
        self.df = self.parse_schema()

    def parse_schema(self):
        """

        :return:
        """
        # Use imported mapping
        df_renamed = self.df.rename(columns=audience_pulse_mapping)

        # Apply schema types
        df_typed = df_renamed.astype(schema)

        return df_typed
