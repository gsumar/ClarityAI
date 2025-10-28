import pandas as pd

from src.movies.models.bronze.data_provider import DataProvider


class CriticAgg(DataProvider):
    def __init__(self, file_path, version=None):
        super().__init__(file_path)
        self.df = pd.read_csv(self.filePath)

        provider_name = 'bronze/critic_agg'
        if version is None:
            version = self.registry.detect_version(provider_name, self.df)

        is_valid, errors = self.registry.validate_schema(provider_name, version, self.df)
        if not is_valid:
            raise ValueError(f"Schema validation failed for {provider_name} {version}: {errors}")

        self.version = version
