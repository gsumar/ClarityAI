from pathlib import Path
import yaml
import argparse

from src.movies.models.bronze.audience_pulse import AudiencePulse as BronzeAudiencePulse
from src.movies.models.bronze.critic_agg import CriticAgg as BronzeCriticAgg
from src.movies.models.bronze.box_office_metrics import BoxOfficeMetrics as BronzeBoxOfficeMetrics

from src.movies.models.silver.audience_pulse import AudiencePulse as SilverAudiencePulse
from src.movies.models.silver.critic_agg import CriticAgg as SilverCriticAgg
from src.movies.models.silver.box_office_metrics import BoxOfficeMetrics as SilverBoxOfficeMetrics

from src.movies.models.gold.movies_unified import MoviesUnified


def load_config(config_path):
    """Load configuration from YAML file"""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def create_target_directories(config):
    """Create target directories for output files"""
    directories = [
        config['output']['bronze'],
        config['output']['silver'],
        config['output']['gold']
    ]
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)


def save_dataframe_to_csv(df, output_path):
    """Save a DataFrame to CSV file"""
    df.to_csv(output_path, index=False)


def process_bronze_layer(config):
    """Process bronze layer - load raw data from source files"""
    bronze_audience_pulse = BronzeAudiencePulse(config['bronze']['audience_pulse'])
    save_dataframe_to_csv(bronze_audience_pulse.df, f"{config['output']['bronze']}/audience_pulse.csv")

    bronze_critic_agg = BronzeCriticAgg(config['bronze']['critic_agg'])
    save_dataframe_to_csv(bronze_critic_agg.df, f"{config['output']['bronze']}/critic_agg.csv")

    bronze_box_office = BronzeBoxOfficeMetrics(
        config['bronze']['box_office']['domestic'],
        config['bronze']['box_office']['financials'],
        config['bronze']['box_office']['international']
    )
    save_dataframe_to_csv(bronze_box_office.domestic_df, f"{config['output']['bronze']}/box_office_domestic.csv")
    save_dataframe_to_csv(bronze_box_office.financials_df, f"{config['output']['bronze']}/box_office_financials.csv")
    save_dataframe_to_csv(bronze_box_office.international_df, f"{config['output']['bronze']}/box_office_international.csv")

    return bronze_audience_pulse, bronze_critic_agg, bronze_box_office


def process_silver_layer(config, bronze_audience_pulse, bronze_critic_agg, bronze_box_office):
    """Process silver layer - transform and standardize data"""
    silver_audience_pulse = SilverAudiencePulse(bronze_audience_pulse.df)
    save_dataframe_to_csv(silver_audience_pulse.df, f"{config['output']['silver']}/audience_pulse.csv")

    silver_critic_agg = SilverCriticAgg(bronze_critic_agg.df)
    save_dataframe_to_csv(silver_critic_agg.df, f"{config['output']['silver']}/critic_agg.csv")

    silver_box_office = SilverBoxOfficeMetrics(
        bronze_box_office.domestic_df,
        bronze_box_office.financials_df,
        bronze_box_office.international_df
    )
    save_dataframe_to_csv(silver_box_office.df, f"{config['output']['silver']}/box_office_metrics.csv")

    return silver_audience_pulse, silver_critic_agg, silver_box_office


def process_gold_layer(config, silver_audience_pulse, silver_critic_agg, silver_box_office):
    """Process gold layer - merge all data into unified dataset"""
    movies_unified = MoviesUnified(silver_audience_pulse, silver_critic_agg, silver_box_office)
    save_dataframe_to_csv(movies_unified.df, f"{config['output']['gold']}/movies_unified.csv")

    return movies_unified


def main(config_path):
    """Main execution function"""
    config = load_config(config_path)

    create_target_directories(config)

    bronze_audience, bronze_critic, bronze_box_office = process_bronze_layer(config)
    silver_audience, silver_critic, silver_box_office = process_silver_layer(
        config, bronze_audience, bronze_critic, bronze_box_office
    )
    process_gold_layer(config, silver_audience, silver_critic, silver_box_office)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Movies Data Pipeline')
    parser.add_argument('config_path', type=str, help='Path to the YAML configuration file')
    args = parser.parse_args()

    main(args.config_path)
