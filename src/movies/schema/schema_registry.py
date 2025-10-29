from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
import json
from pathlib import Path
import pandas as pd


@dataclass
class SchemaVersion:
    """Represents a specific version of a provider's schema"""
    version: str
    description: str
    schema: Dict[str, str]  # column_name -> data_type
    mapping: Dict[str, str]  # source_column -> target_column
    transformations: Dict[str, str] = field(default_factory=dict)  # column -> transformation_type

    def apply_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply column mapping to a DataFrame"""
        return df.rename(columns=self.mapping)

    def apply_transformations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data type transformations to a DataFrame"""
        df = df.copy()
        for column, transform_type in self.transformations.items():
            if column in df.columns:
                if transform_type == "int":
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                elif transform_type == "float":
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif transform_type == "string":
                    df[column] = df[column].astype(str)
        return df


class SchemaRegistry:
    """
    Centralized schema management for all data providers.

    Supports:
    - Multiple schema versions per provider
    - Automatic version detection
    - Schema validation
    - Column mapping and transformations
    """

    def __init__(self, schema_dir: str = None):
        if schema_dir is None:
            # Default to versions directory relative to this file
            schema_dir = Path(__file__).parent / "versions"
        self.schema_dir = Path(schema_dir).resolve()
        self.schemas: Dict[str, Dict[str, SchemaVersion]] = {}
        self._load_schemas()

    def _load_schemas(self):
        """Load all schema versions from JSON files (supports nested directories)"""
        if not self.schema_dir.exists():
            raise FileNotFoundError(f"Schema directory not found: {self.schema_dir}")

        self._load_schemas_recursive(self.schema_dir, "")

    def _load_schemas_recursive(self, directory, prefix):
        """Recursively load schemas from nested directories"""
        for item in directory.iterdir():
            if item.is_dir():
                # Check if this directory contains schema files
                schema_files = list(item.glob("v*.json"))
                if schema_files:
                    # This is a provider directory with schemas
                    provider_name = f"{prefix}/{item.name}" if prefix else item.name
                    provider_schemas = {}

                    for schema_file in sorted(schema_files):
                        version = schema_file.stem
                        try:
                            with open(schema_file) as f:
                                schema_data = json.load(f)

                            # Ensure transformations field exists
                            if 'transformations' not in schema_data:
                                schema_data['transformations'] = {}

                            provider_schemas[version] = SchemaVersion(**schema_data)
                        except Exception as e:
                            print(f"Warning: Failed to load schema {schema_file}: {e}")

                    if provider_schemas:
                        self.schemas[provider_name] = provider_schemas
                else:
                    # Recurse into subdirectory
                    new_prefix = f"{prefix}/{item.name}" if prefix else item.name
                    self._load_schemas_recursive(item, new_prefix)

    def get_schema(self, provider: str, version: str) -> Optional[SchemaVersion]:
        """Get a specific schema version for a provider"""
        return self.schemas.get(provider, {}).get(version)

    def get_latest_version(self, provider: str) -> Optional[str]:
        """Get the latest version number for a provider"""
        provider_schemas = self.schemas.get(provider, {})
        if not provider_schemas:
            return None
        # Versions are like v1, v2, v3... sort them
        versions = sorted(provider_schemas.keys(), key=lambda x: int(x[1:]))
        return versions[-1] if versions else None

    def list_versions(self, provider: str) -> List[str]:
        """List all available versions for a provider"""
        return list(self.schemas.get(provider, {}).keys())

    def detect_version(self, provider: str, df: pd.DataFrame) -> str:
        """
        Auto-detect schema version based on DataFrame columns.
        Returns the first matching version or 'unknown'.
        """
        provider_schemas = self.schemas.get(provider, {})

        # Try versions in reverse order (newest first)
        for version in sorted(provider_schemas.keys(), key=lambda x: int(x[1:]), reverse=True):
            schema = provider_schemas[version]
            expected_cols = set(schema.schema.keys())
            actual_cols = set(df.columns)

            # Check if all expected columns are present
            if expected_cols.issubset(actual_cols):
                return version

        return "unknown"

    def validate_schema(self, provider: str, version: str, df: pd.DataFrame) -> tuple[bool, List[str]]:
        """
        Validate a DataFrame against a schema version.
        Returns (is_valid, list_of_errors)
        """
        schema = self.get_schema(provider, version)
        if not schema:
            return False, [f"Schema not found: {provider}/{version}"]

        errors = []
        expected_cols = set(schema.schema.keys())
        actual_cols = set(df.columns)

        # Check for missing columns
        missing_cols = expected_cols - actual_cols
        if missing_cols:
            errors.append(f"Missing columns: {missing_cols}")

        # Check for unexpected columns (warning, not error)
        extra_cols = actual_cols - expected_cols
        if extra_cols:
            errors.append(f"Extra columns (ignored): {extra_cols}")

        return len(errors) == 0, errors

    def transform_dataframe(self, provider: str, version: str, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply full transformation pipeline: mapping + transformations.
        """
        schema = self.get_schema(provider, version)
        if not schema:
            available = list(self.schemas.keys())
            raise ValueError(f"Schema not found: {provider}/{version}. Available providers: {available}")

        # Apply transformations first (on source columns)
        df = schema.apply_transformations(df)

        # Then apply mapping (rename columns)
        df = schema.apply_mapping(df)

        return df