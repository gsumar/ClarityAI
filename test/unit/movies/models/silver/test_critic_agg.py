import pytest
import pandas as pd
from src.movies.models.silver.critic_agg import CriticAgg


class TestCriticAgg:
    @pytest.fixture
    def bronze_df(self):
        """Create a sample bronze DataFrame with correct column names"""
        return pd.DataFrame(
            {
                'movie_title': ['my_film_test_1', 'my_film_test_2'],
                'release_year': [1998, 2005],
                'critic_score_percentage': [12, 85],
                'top_critic_score': [7.5, 8.9],
                'total_critic_reviews_counted': [150, 320],
            }
        )

    @pytest.fixture
    def critic_agg(self, bronze_df):
        return CriticAgg(bronze_df)

    def test_init_parses_schema(self, critic_agg):
        """Test that schema parsing is applied during initialization"""
        assert isinstance(critic_agg.df, pd.DataFrame)
        assert not critic_agg.df.empty

    def test_expected_columns(self, critic_agg):
        """Test that expected columns are present"""
        expected_columns = [
            'movie_title',
            'release_year',
            'critic_score_percentage',
            'top_critic_score',
            'total_critic_reviews_counted',
        ]
        assert list(critic_agg.df.columns) == expected_columns

    def test_data_types(self, critic_agg):
        """Test that data types are correctly applied"""
        assert str(critic_agg.df['movie_title'].dtype) in [
            'object',
            'string',
            'string[python]',
        ]
        assert critic_agg.df['release_year'].dtype == 'int64'
        assert critic_agg.df['critic_score_percentage'].dtype == 'int64'
        assert critic_agg.df['top_critic_score'].dtype == 'float64'
        assert critic_agg.df['total_critic_reviews_counted'].dtype == 'int64'

    def test_data_content_preserved(self, critic_agg):
        """Test that data values are preserved after transformation"""
        first_row = critic_agg.df.iloc[0]
        assert first_row['movie_title'] == 'my_film_test_1'
        assert first_row['release_year'] == 1998
        assert first_row['critic_score_percentage'] == 12
        assert first_row['top_critic_score'] == 7.5
        assert first_row['total_critic_reviews_counted'] == 150

    def test_parse_schema_method(self, bronze_df):
        """Test that parse_schema method works correctly"""
        critic_agg = CriticAgg(bronze_df)

        # Verify the schema was parsed during initialization
        assert isinstance(critic_agg.df, pd.DataFrame)
        assert critic_agg.df['release_year'].dtype == 'int64'
        assert critic_agg.df['top_critic_score'].dtype == 'float64'
