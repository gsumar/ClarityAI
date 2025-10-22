import pytest
import pandas as pd

from src.movies.models.critic_agg import CriticAgg


class TestCriticAgg:
    @pytest.fixture
    def test_file_path(self):
        return "test/data/CriticAgg/test_provider1.csv"

    @pytest.fixture
    def critic_agg(self, test_file_path):
        return CriticAgg(test_file_path)

    def test_init_loads_csv(self, critic_agg):
        """Test that CSV file is loaded correctly"""
        assert isinstance(critic_agg.df, pd.DataFrame)
        assert not critic_agg.df.empty

    def test_expected_schema(self, critic_agg):
        """Test that expected columns are present"""
        expected_columns = [
            'movie_title',
            'release_year',
            'critic_score_percentage',
            'top_critic_score',
            'total_critic_reviews_counted',
        ]
        assert list(critic_agg.df.columns) == expected_columns

    def test_data_content(self, critic_agg):
        """Test specific data values"""
        first_row = critic_agg.df.iloc[0]
        assert first_row['movie_title'] == 'my_film_test_1'
        assert first_row['release_year'] == 1998
        assert first_row['critic_score_percentage'] == 12

    def test_parse_schema_method_exists(self, critic_agg):
        """Test that parse_schema method can be called"""
        # Should not raise an exception
        critic_agg.parse_schema()
