from abc import ABC, abstractmethod
import pandas as pd


class AnalyticsReport(ABC):
    """
    Abstract base class for analytics reports in the gold layer.
    All gold layer reports must implement the build_report method.
    """

    @abstractmethod
    def build_report(self) -> pd.DataFrame:
        """
        Build and return the analytics report as a DataFrame.

        :return: DataFrame containing the analytics report
        """

