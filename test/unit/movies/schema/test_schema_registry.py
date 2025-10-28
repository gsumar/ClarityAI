import pytest
import pandas as pd
from src.movies.schema.schema_registry import SchemaRegistry, SchemaVersion


class TestSchemaRegistry:
    @pytest.fixture
    def registry(self):
        """Create a SchemaRegistry instance"""
        return SchemaRegistry()
    
    def test_load_schemas(self, registry):
        """Test that schemas are loaded from files"""
        assert len(registry.schemas) > 0
        assert 'silver/audience_pulse' in registry.schemas
        assert 'silver/critic_agg' in registry.schemas
        assert 'silver/box_office' in registry.schemas

    def test_get_schema(self, registry):
        """Test retrieving a specific schema version"""
        schema = registry.get_schema('silver/audience_pulse', 'v1')
        assert schema is not None
        assert schema.version == 'v1'
        assert 'title' in schema.schema
        assert 'year' in schema.schema

    def test_get_latest_version(self, registry):
        """Test getting the latest version for a provider"""
        latest = registry.get_latest_version('silver/audience_pulse')
        assert latest == 'v1'

    def test_list_versions(self, registry):
        """Test listing all versions for a provider"""
        versions = registry.list_versions('silver/audience_pulse')
        assert 'v1' in versions

    def test_detect_version(self, registry):
        """Test automatic version detection"""
        # Create a DataFrame with v1 schema columns
        df = pd.DataFrame({
            'title': ['Inception'],
            'year': ['2010'],
            'audience_average_score': [9.1],
            'total_audience_ratings': [1500000],
            'domestic_box_office_gross': [292576195]
        })

        version = registry.detect_version('silver/audience_pulse', df)
        assert version == 'v1'

    def test_detect_version_unknown(self, registry):
        """Test version detection with unknown schema"""
        df = pd.DataFrame({
            'unknown_column': ['value']
        })

        version = registry.detect_version('silver/audience_pulse', df)
        assert version == 'unknown'

    def test_validate_schema_valid(self, registry):
        """Test schema validation with valid data"""
        df = pd.DataFrame({
            'title': ['Inception'],
            'year': ['2010'],
            'audience_average_score': [9.1],
            'total_audience_ratings': [1500000],
            'domestic_box_office_gross': [292576195]
        })

        is_valid, errors = registry.validate_schema('silver/audience_pulse', 'v1', df)
        assert is_valid is True
        assert len(errors) == 0

    def test_validate_schema_missing_columns(self, registry):
        """Test schema validation with missing columns"""
        df = pd.DataFrame({
            'title': ['Inception']
        })

        is_valid, errors = registry.validate_schema('silver/audience_pulse', 'v1', df)
        assert is_valid is False
        assert len(errors) > 0

    def test_apply_mapping(self, registry):
        """Test applying column mapping"""
        schema = registry.get_schema('silver/audience_pulse', 'v1')

        df = pd.DataFrame({
            'title': ['Inception'],
            'year': ['2010'],
            'audience_average_score': [9.1],
            'total_audience_ratings': [1500000]
        })

        mapped_df = schema.apply_mapping(df)
        assert 'movie_title' in mapped_df.columns
        assert 'release_year' in mapped_df.columns
        assert 'title' not in mapped_df.columns

    def test_apply_transformations(self, registry):
        """Test applying data type transformations"""
        schema = registry.get_schema('silver/audience_pulse', 'v1')

        df = pd.DataFrame({
            'title': ['Inception'],
            'year': ['2010'],
            'audience_average_score': [9.1],
            'total_audience_ratings': [1500000]
        })

        transformed_df = schema.apply_transformations(df)
        # Year should be converted to int
        assert transformed_df['year'].dtype == 'Int64'

    def test_transform_dataframe(self, registry):
        """Test full transformation pipeline"""
        df = pd.DataFrame({
            'title': ['Inception'],
            'year': ['2010'],
            'audience_average_score': [9.1],
            'total_audience_ratings': [1500000]
        })

        transformed_df = registry.transform_dataframe('silver/audience_pulse', 'v1', df)

        # Check columns are renamed
        assert 'movie_title' in transformed_df.columns
        assert 'release_year' in transformed_df.columns

        # Check transformations are applied
        assert transformed_df['release_year'].dtype == 'Int64'

        # Check values are preserved
        assert transformed_df['movie_title'].iloc[0] == 'Inception'
        assert transformed_df['release_year'].iloc[0] == 2010

