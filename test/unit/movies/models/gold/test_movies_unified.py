import pytest
import pandas as pd
from src.movies.models.gold.movies_unified import MoviesUnified


class TestMoviesUnified:
    @pytest.fixture
    def audience_pulse_df(self):
        """Create a sample audience pulse DataFrame"""
        return pd.DataFrame({
            'movie_title': ['Movie A', 'Movie B', 'Movie C'],
            'release_year': [2020, 2021, 2022],
            'critic_score_percentage': [85, 72, 90],
            'top_critic_score': [8.5, 7.2, 9.0],
            'total_critic_reviews_counted': [150, 200, 180]
        })

    @pytest.fixture
    def critic_agg_df(self):
        """Create a sample critic agg DataFrame"""
        return pd.DataFrame({
            'movie_title': ['Movie A', 'Movie B', 'Movie D'],
            'release_year': [2020, 2021, 2023],
            'critic_score_percentage': [88, 75, 82],
            'top_critic_score': [8.8, 7.5, 8.2],
            'total_critic_reviews_counted': [160, 210, 140]
        })

    @pytest.fixture
    def box_office_metrics_df(self):
        """Create a sample box office metrics DataFrame"""
        return pd.DataFrame({
            'movie_title': ['Movie A', 'Movie C', 'Movie D'],
            'release_year': [2020, 2022, 2023],
            'total_box_office_gross_usd': [500000000, 750000000, 300000000],
            'production_budget_usd': [150000000, 200000000, 100000000],
            'marketing_spend_usd': [80000000, 100000000, 50000000]
        })

    @pytest.fixture
    def mock_audience_pulse(self, audience_pulse_df):
        """Create a mock AudiencePulse object"""
        class MockAudiencePulse:
            def __init__(self, df):
                self.df = df
        return MockAudiencePulse(audience_pulse_df)

    @pytest.fixture
    def mock_critic_agg(self, critic_agg_df):
        """Create a mock CriticAgg object"""
        class MockCriticAgg:
            def __init__(self, df):
                self.df = df
        return MockCriticAgg(critic_agg_df)

    @pytest.fixture
    def mock_box_office_metrics(self, box_office_metrics_df):
        """Create a mock BoxOfficeMetrics object"""
        class MockBoxOfficeMetrics:
            def __init__(self, df):
                self.df = df
        return MockBoxOfficeMetrics(box_office_metrics_df)

    def test_output_matches_expected_csv(self, mock_audience_pulse, mock_critic_agg, mock_box_office_metrics):
        """Test that the output matches the expected CSV file"""
        movies_unified = MoviesUnified(mock_audience_pulse, mock_critic_agg, mock_box_office_metrics)
        actual_df = movies_unified.df

        expected_df = pd.read_csv('test/data/movies_unified/expected_gold_output.csv')

        pd.testing.assert_frame_equal(actual_df, expected_df)

