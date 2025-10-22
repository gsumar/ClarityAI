import pandas as pd

from src.movies.models.bronze.data_provider import DataProvider


class BoxOfficeMetrics(DataProvider):
    def __init__(
        self, domestic_file_path, financials_file_path, international_file_path
    ):
        super().__init__(domestic_file_path)
        self.domestic_df = pd.read_csv(domestic_file_path)
        self.financials_df = pd.read_csv(financials_file_path)
        self.international_df = pd.read_csv(international_file_path)
