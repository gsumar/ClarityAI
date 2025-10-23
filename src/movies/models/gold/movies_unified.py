import pandas as pd

from src.movies.models.gold.analytics_reports import AnalyticsReport
from src.movies.schema.gold_schema import gold_column_mapping


class MoviesUnified(AnalyticsReport):
    def __init__(self, audience_pulse, critic_agg, box_office_metrics):
        self.audience_pulse = audience_pulse
        self.critic_agg = critic_agg
        self.box_office_metrics = box_office_metrics
        self.df = self.build_report()

    def build_report(self) -> pd.DataFrame:
        """
        Merge all silver layer data into a unified DataFrame

        :return: Unified DataFrame with all movie data
        """
        unified_df = self.audience_pulse.df.copy()

        unified_df = pd.merge(
            unified_df,
            self.critic_agg.df,
            on='movie_title',
            how='outer',
            suffixes=('_audience', '_critic')
        )

        unified_df = pd.merge(
            unified_df,
            self.box_office_metrics.df,
            on='movie_title',
            how='outer'
        )

        unified_df = unified_df.rename(columns=gold_column_mapping)

        return unified_df.sort_values('movie_title').reset_index(drop=True)