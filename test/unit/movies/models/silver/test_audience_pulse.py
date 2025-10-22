import pytest
import pandas as pd
from src.movies.models.silver.audience_pulse import AudiencePulse


class TestAudiencePulse:
    @pytest.fixture
    def bronze_df(self):
        """Create a sample bronze DataFrame with original column names"""
        return pd.DataFrame({
            'title': ['Test Movie A', 'Test Movie B'],
            'year': [2020, 2021],
            'audience_average_score': [8.5, 7.8],
            'total_audience_ratings': [500000, 750000],
            'domestic_box_office_gross': [150000000, 200000000]
        })

    @pytest.fixture
    def audience_pulse(self, bronze_df):
        return AudiencePulse(bronze_df)

    def test_init_parses_schema(self, audience_pulse):
        """Test that schema parsing is applied during initialization"""
        assert isinstance(audience_pulse.df, pd.DataFrame)
        assert not audience_pulse.df.empty

    def test_column_mapping(self, audience_pulse):
        """Test that columns are renamed according to the schema mapping"""
        expected_columns = [
            'movie_title',
            'release_year',
            'critic_score_percentage',
            'top_critic_score',
            'total_critic_reviews_counted',
        ]
        assert list(audience_pulse.df.columns) == expected_columns

    def test_data_types(self, audience_pulse):
        """Test that data types are correctly applied"""
        # Check string dtype (can be 'object' or 'string[python]')
        assert str(audience_pulse.df['movie_title'].dtype) in ['object', 'string', 'string[python]']
        assert audience_pulse.df['release_year'].dtype == 'int64'
        assert audience_pulse.df['critic_score_percentage'].dtype == 'int64'
        assert audience_pulse.df['top_critic_score'].dtype == 'float64'
        assert audience_pulse.df['total_critic_reviews_counted'].dtype == 'int64'

    def test_data_content_preserved(self, audience_pulse):
        """Test that data values are preserved after transformation"""
        first_row = audience_pulse.df.iloc[0]
        assert first_row['movie_title'] == 'Test Movie A'
        assert first_row['release_year'] == 2020
        assert first_row['critic_score_percentage'] == 8
        assert first_row['top_critic_score'] == 500000.0
        assert first_row['total_critic_reviews_counted'] == 150000000

    def test_parse_schema_method(self, bronze_df):
        """Test that parse_schema method works correctly"""
        audience_pulse = AudiencePulse(bronze_df)

        # Verify the schema was parsed during initialization
        assert isinstance(audience_pulse.df, pd.DataFrame)
        assert 'movie_title' in audience_pulse.df.columns
        assert 'title' not in audience_pulse.df.columns

