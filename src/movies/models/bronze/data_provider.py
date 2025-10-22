from abc import abstractmethod, ABC


class DataProvider(ABC):
    def __init__(self, file_path):
        self.filePath = file_path
