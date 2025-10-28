import pandas as pd

from src.movies.models.gold.analytics_reports import AnalyticsReport


class MoviesUnified(AnalyticsReport):
    def __init__(self, audience_pulse, critic_agg, box_office_metrics, version='v1'):
        super().__init__()
        self.audience_pulse = audience_pulse
        self.critic_agg = critic_agg
        self.box_office_metrics = box_office_metrics
        self.version = version
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

        # Use Schema Registry to apply gold layer column mapping
        schema = self.registry.get_schema('gold/movies_unified', self.version)
        if schema:
            unified_df = schema.apply_mapping(unified_df)

        return unified_df.sort_values('movie_title').reset_index(drop=True)