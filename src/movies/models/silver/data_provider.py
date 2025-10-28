from abc import abstractmethod, ABC
from src.movies.schema.schema_registry import SchemaRegistry


class DataProvider(ABC):
    def __init__(self, df):
        self.df = df
        self.registry = SchemaRegistry()

    @abstractmethod
    def parse_schema(self):
        pass
