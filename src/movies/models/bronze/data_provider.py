from abc import ABC
from src.movies.schema.schema_registry import SchemaRegistry


class DataProvider(ABC):
    def __init__(self, file_path):
        self.filePath = file_path
        self.registry = SchemaRegistry()
