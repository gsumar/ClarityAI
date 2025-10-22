from abc import abstractmethod, ABC


class DataProvider(ABC):
    def __init__(self, df):
        self.df = df

    @abstractmethod
    def parse_schema(self):
        pass
