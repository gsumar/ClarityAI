import pytest
import pandas as pd
from src.movies.models.bronze.box_office_metrics import BoxOfficeMetrics


class TestBoxOfficeMetrics:
    @pytest.fixture
    def box_office_metrics(self):
        return BoxOfficeMetrics(
            "test/data/box_office_metrics/test_provider3_domestic.csv",
            "test/data/box_office_metrics/test_provider3_financials.csv",
            "test/data/box_office_metrics/test_provider3_international.csv",
        )

    def test_init_loads_domestic_csv(self, box_office_metrics):
        """Test that domestic CSV file is loaded correctly"""
        assert isinstance(box_office_metrics.domestic_df, pd.DataFrame)
        assert not box_office_metrics.domestic_df.empty

    def test_init_loads_financials_csv(self, box_office_metrics):
        """Test that financials CSV file is loaded correctly"""
        assert isinstance(box_office_metrics.financials_df, pd.DataFrame)
        assert not box_office_metrics.financials_df.empty

    def test_init_loads_international_csv(self, box_office_metrics):
        """Test that international CSV file is loaded correctly"""
        assert isinstance(box_office_metrics.international_df, pd.DataFrame)
        assert not box_office_metrics.international_df.empty

    def test_domestic_expected_columns(self, box_office_metrics):
        """Test that domestic dataframe has expected columns"""
        expected_columns = [
            'film_name',
            'year_of_release',
            'box_office_gross_usd',
        ]
        assert list(box_office_metrics.domestic_df.columns) == expected_columns

    def test_international_expected_columns(self, box_office_metrics):
        """Test that international dataframe has expected columns"""
        expected_columns = [
            'film_name',
            'year_of_release',
            'box_office_gross_usd',
        ]
        assert list(box_office_metrics.international_df.columns) == expected_columns

    def test_financials_expected_columns(self, box_office_metrics):
        """Test that financials dataframe has expected columns"""
        expected_columns = [
            'film_name',
            'year_of_release',
            'production_budget_usd',
            'marketing_spend_usd',
        ]
        assert list(box_office_metrics.financials_df.columns) == expected_columns

    def test_domestic_data_content(self, box_office_metrics):
        """Test specific data values in domestic dataframe"""
        first_row = box_office_metrics.domestic_df.iloc[0]
        assert first_row['film_name'] == 'Test Film A'
        assert first_row['year_of_release'] == 2020
        assert first_row['box_office_gross_usd'] == 250000000

    def test_international_data_content(self, box_office_metrics):
        """Test specific data values in international dataframe"""
        first_row = box_office_metrics.international_df.iloc[0]
        assert first_row['film_name'] == 'Test Film A'
        assert first_row['year_of_release'] == 2020
        assert first_row['box_office_gross_usd'] == 450000000

    def test_financials_data_content(self, box_office_metrics):
        """Test specific data values in financials dataframe"""
        first_row = box_office_metrics.financials_df.iloc[0]
        assert first_row['film_name'] == 'Test Film A'
        assert first_row['year_of_release'] == 2020
        assert first_row['production_budget_usd'] == 150000000
        assert first_row['marketing_spend_usd'] == 80000000
