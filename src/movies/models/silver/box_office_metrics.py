import pandas as pd

from src.movies.models.silver.data_provider import DataProvider
from src.movies.schema.movies_merge_schema import box_office_metrics_mapping


class BoxOfficeMetrics(DataProvider):
    def __init__(self, domestic_df, financials_df, international_df):
        self.domestic_df = domestic_df
        self.financials_df = financials_df
        self.international_df = international_df
        self.df = self.parse_schema()
        super().__init__(self.df)

    def parse_schema(self):
        """
        Generate the model merging the data in the three box_office files

        :return: Merged and transformed DataFrame
        """
        # Merge domestic and international on film_name only
        merged_df = pd.merge(
            self.domestic_df,
            self.international_df,
            on='film_name',
            how='inner',
            suffixes=('_domestic', '_international')
        )

        # Calculate total box office gross (domestic + international)
        merged_df['total_box_office_gross_usd'] = (
            merged_df['box_office_gross_usd_domestic'] + merged_df['box_office_gross_usd_international']
        )

        # Merge with financials
        merged_df = pd.merge(
            merged_df,
            self.financials_df,
            on='film_name',
            how='inner'
        )

        # Select and order columns before renaming
        # Use year_of_release_domestic (could also use _international, they should be the same)
        merged_df = merged_df[[
            'film_name',
            'year_of_release_domestic',
            'total_box_office_gross_usd',
            'production_budget_usd',
            'marketing_spend_usd'
        ]]

        # Rename columns once at the end
        return merged_df.rename(columns=box_office_metrics_mapping)
