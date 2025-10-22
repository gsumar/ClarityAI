import pytest
import pandas as pd
from src.movies.models.audience_pulse import AudiencePulse


class TestAudiencePulse:
    @pytest.fixture
    def test_file_path(self):
        return "test/data/AudiencePulse/test_provider2.json"

    @pytest.fixture
    def audience_pulse(self, test_file_path):
        return AudiencePulse(test_file_path)

    def test_init_loads_json(self, audience_pulse):
        """Test that JSON file is loaded correctly"""
        assert isinstance(audience_pulse.df, pd.DataFrame)
        assert not audience_pulse.df.empty

    def test_expected_columns(self, audience_pulse):
        """Test that expected columns are present"""
        expected_columns = [
            'title',
            'year',
            'audience_average_score',
            'total_audience_ratings',
            'domestic_box_office_gross',
        ]
        assert list(audience_pulse.df.columns) == expected_columns

    def test_data_content(self, audience_pulse):
        """Test specific data values"""
        first_row = audience_pulse.df.iloc[0]
        assert first_row['title'] == 'Test Movie A'
        assert first_row['year'] == 2020
        assert first_row['audience_average_score'] == 8.5
        assert first_row['total_audience_ratings'] == 500000

    def test_parse_schema_method_exists(self, audience_pulse):
        """Test that parse_schema method can be called"""
        audience_pulse.parse_schema()
