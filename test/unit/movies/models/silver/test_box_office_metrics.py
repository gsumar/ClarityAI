import pytest
import pandas as pd
from src.movies.models.silver.box_office_metrics import BoxOfficeMetrics


class TestBoxOfficeMetrics:
    @pytest.fixture
    def domestic_df(self):
        """Create a sample domestic DataFrame"""
        return pd.DataFrame({
            'film_name': ['Test Film A', 'Test Film B'],
            'year_of_release': [2020, 2021],
            'box_office_gross_usd': [250000000, 180000000]
        })

    @pytest.fixture
    def financials_df(self):
        """Create a sample financials DataFrame"""
        return pd.DataFrame({
            'film_name': ['Test Film A', 'Test Film B'],
            'year_of_release': [2020, 2021],
            'production_budget_usd': [150000000, 120000000],
            'marketing_spend_usd': [80000000, 60000000]
        })

    @pytest.fixture
    def international_df(self):
        """Create a sample international DataFrame"""
        return pd.DataFrame({
            'film_name': ['Test Film A', 'Test Film B'],
            'year_of_release': [2020, 2021],
            'box_office_gross_usd': [450000000, 320000000]
        })

    @pytest.fixture
    def box_office_metrics(self, domestic_df, financials_df, international_df):
        return BoxOfficeMetrics(domestic_df, financials_df, international_df)

    def test_result_number_of_records(self, box_office_metrics):
        """Test that schema parsing is applied during initialization"""
        assert len(box_office_metrics.df) == 2

    def test_expected_columns(self, box_office_metrics):
        """Test that expected columns are present"""
        expected_columns = [
            'movie_title',
            'release_year',
            'total_box_office_gross_usd',
            'production_budget_usd',
            'marketing_spend_usd',
        ]
        assert list(box_office_metrics.df.columns) == expected_columns

    def test_total_box_office_calculation(self, box_office_metrics):
        """Test that total box office is correctly calculated (domestic + international)"""
        first_row = box_office_metrics.df.iloc[0]
        assert first_row['total_box_office_gross_usd'] == 700000000
        
        second_row = box_office_metrics.df.iloc[1]
        assert second_row['total_box_office_gross_usd'] == 500000000

    def test_financials_data_included(self, box_office_metrics):
        """Test that financial data is correctly included"""
        first_row = box_office_metrics.df.iloc[0]
        assert first_row['production_budget_usd'] == 150000000
        assert first_row['marketing_spend_usd'] == 80000000
        
        second_row = box_office_metrics.df.iloc[1]
        assert second_row['production_budget_usd'] == 120000000
        assert second_row['marketing_spend_usd'] == 60000000

    def test_data_content_preserved(self, box_office_metrics):
        """Test that data values are preserved after transformation"""
        first_row = box_office_metrics.df.iloc[0]
        assert first_row['movie_title'] == 'Test Film A'
        assert first_row['release_year'] == 2020

    def test_all_rows_merged(self, box_office_metrics):
        """Test that all rows are present after merge"""
        assert len(box_office_metrics.df) == 2

    def test_parse_schema_method(self, domestic_df, financials_df, international_df):
        """Test that parse_schema method works correctly"""
        box_office_metrics = BoxOfficeMetrics(domestic_df, financials_df, international_df)

        # Verify the schema was parsed during initialization
        assert isinstance(box_office_metrics.df, pd.DataFrame)
        assert 'movie_title' in box_office_metrics.df.columns
        assert 'total_box_office_gross_usd' in box_office_metrics.df.columns

    def test_output_matches_expected_csv(self, box_office_metrics):
        """Test that the output matches the expected CSV file"""
        expected_df = pd.read_csv('test/data/box_office_metrics/expected_silver_output.csv')

        result_df = box_office_metrics.df.sort_values('movie_title')
        expected_df = expected_df.sort_values('movie_title')

        pd.testing.assert_frame_equal(result_df, expected_df)

