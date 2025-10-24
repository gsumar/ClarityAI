# Movies Data Pipeline

A data pipeline built using the **Medallion Architecture** to process and unify movie data from multiple sources (audience ratings, critic reviews, and box office metrics) into analytics-ready reports.

## 📊 Architecture Overview

This project implements the **Medallion Architecture** (Bronze → Silver → Gold), a data engineering pattern that progressively refines data through multiple layers:

```
┌─────────────┐
│   Sources   │  Raw data files from different providers
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Bronze    │  Raw data ingestion (no transformations)
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Silver    │  Cleaned, validated, and standardized data
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Gold     │  Business-ready analytics reports
└─────────────┘
```

### 🥉 Bronze Layer
**Purpose**: Raw data ingestion

- Loads data exactly as it arrives from source systems
- No transformations or cleaning
- Preserves original data structure and values
- Acts as a historical archive of raw data

**Models**:
- `BronzeAudiencePulse`: Audience ratings from Provider 2 (JSON format)
- `BronzeCriticAgg`: Critic reviews from Provider 1 (CSV format)
- `BronzeBoxOfficeMetrics`: Box office data from Provider 3 (3 CSV files: domestic, international, financials)

### 🥈 Silver Layer
**Purpose**: Cleaned and standardized data

- Removes corrupted or invalid values
- Standardizes column names across different providers
- Applies consistent data types
- Validates data quality
- Merges related datasets (e.g., box office domestic + international + financials)

**Models**:
- `SilverAudiencePulse`: Standardized audience ratings
- `SilverCriticAgg`: Standardized critic reviews
- `SilverBoxOfficeMetrics`: Unified box office metrics

### 🥇 Gold Layer
**Purpose**: Analytics-ready reports

- Aggregated and enriched data
- Optimized for business intelligence and analytics
- Ready for consumption by data analysts and business users
- Implements business logic and KPIs

**Models**:
- `MoviesUnified`: Unified view of all movie data (audience + critic + box office)

## 🚀 How to Run

### Prerequisites
- Python 3.11+
- Poetry (for dependency management)

### Installation
```bash
poetry install
```

### Running the Pipeline

The pipeline accepts a YAML configuration file as a command-line argument:

```bash
python src/movies/main.py input.yaml
```

### Configuration File Format

Create a YAML file (e.g., `input.yaml`) with the following structure:

```yaml
# Source data files
source:
  audience_pulse: ${audience_pulse_file_path}
  critic_agg: ${critic_agg_file_path}
  box_office:
    domestic: ${box_office_domestic_file_path}
    financials: ${box_office_financials_file_path}
    international: ${box_office_international_file_path}

# Output directories
output:
  bronze: ${bronze_output_dir}
  silver: ${silver_output_dir}
  gold: ${gold_output_dir}
```

### Output

The pipeline generates CSV files at each layer:

**Bronze Layer**:
- `audience_pulse.csv`
- `critic_agg.csv`
- `box_office_domestic.csv`
- `box_office_international.csv`
- `box_office_financials.csv`

**Silver Layer**:
- `audience_pulse.csv`
- `critic_agg.csv`
- `box_office_metrics.csv`

**Gold Layer**:
- `movies_unified.csv` - Final analytics report with all data unified

> **Note**: In a production environment, these outputs can be written using **dbt** (data build tool) and stored in optimized formats like **Parquet** or **Iceberg** tables in **S3** or other cloud storage. This provides better performance, compression, and enables features like time travel and schema evolution.

## 🔄 Production Deployment

### Event-Driven Architecture

In a real production scenario, files arrive at different frequencies from different providers. Instead of running the pipeline manually, you can use event-driven orchestration:

#### Option 1: Apache Airflow
```python
# Example DAG structure
audience_pulse_sensor >> process_bronze_audience >> process_silver_audience
critic_agg_sensor >> process_bronze_critic >> process_silver_critic
box_office_sensor >> process_bronze_box_office >> process_silver_box_office

[process_silver_audience, process_silver_critic, process_silver_box_office] >> process_gold_unified
```

#### Option 2: AWS EventBridge + Lambda
- **S3 Event Notifications**: Trigger Lambda functions when new files arrive in S3
- **EventBridge Rules**: Orchestrate multi-step processing
- **Step Functions**: Coordinate complex workflows across layers

```
S3 Upload → EventBridge → Lambda (Bronze) → EventBridge → Lambda (Silver) → EventBridge → Lambda (Gold)
```

## 📦 Adding New Data Sources

The architecture is designed to be easily extensible. To add a new data source:

### 1. Create Bronze Model
```python
# src/movies/models/bronze/new_source.py
import pandas as pd

class NewSource:
    def __init__(self, file_path):
        self.df = pd.read_csv(file_path)  # or read_json, etc.
```

### 2. Create Silver Model
```python
# src/movies/models/silver/new_source.py
from src.movies.schema.movies_merge_schema import new_source_mapping

class NewSource:
    def __init__(self, bronze_df):
        self.df = bronze_df.rename(columns=new_source_mapping)
        # Add data cleaning and validation logic
```

### 3. Update Gold Reports
```python
# src/movies/models/gold/movies_unified.py
# Add the new source to the merge logic
unified_df = pd.merge(unified_df, new_source.df, on='movie_title', how='outer')
```

### 4. Update Configuration
```yaml
source:
  new_source: path/to/new_source.csv
```

### 5. Update main.py
Add processing logic for the new source in the bronze and silver layer functions.

That's it! The modular design makes it easy to integrate new data sources without affecting existing pipelines.

## 🧪 Testing

### Run All Tests
```bash
pytest test/ -v
```

### Run Unit Tests Only
```bash
pytest test/unit/ -v
```

### Run Integration Tests Only
```bash
pytest test/integration/ -v
```

### Test Structure
- **Unit Tests**: Test individual models in isolation with mock data
- **Integration Tests**: Test the entire pipeline end-to-end with real data

## 🎯 Key Features

- ✅ **Medallion Architecture**: Industry-standard data lakehouse pattern
- ✅ **Modular Design**: Easy to add new data sources
- ✅ **Configuration-Driven**: YAML-based configuration for flexibility
- ✅ **Type Safety**: Abstract base classes enforce contracts
- ✅ **Comprehensive Testing**: Unit and integration tests
- ✅ **Production-Ready**: Designed for event-driven orchestration

## 💡 Future Enhancements

### Containerized Data Lakehouse Architecture

For a more sophisticated approach, the entire pipeline can be orchestrated using **Docker containers** to create a complete data lakehouse environment:

#### Architecture Components

```
┌─────────────────────────────────────────────────────────────┐
│                     Docker Compose Stack                     │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐      ┌──────────────┐      ┌───────────┐ │
│  │     dbt      │ ───> │    MinIO     │ <─── │   Trino   │ │
│  │  (Transform) │      │  (S3 Store)  │      │  (Query)  │ │
│  └──────────────┘      └──────────────┘      └───────────┘ │
│         │                      │                     │       │
│         │                      │                     │       │
│         v                      v                     v       │
│  Bronze/Silver/Gold      Parquet/Iceberg        SQL API     │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

#### Implementation Overview

**1. dbt Container**
- Run transformations for Bronze → Silver → Gold layers
- Write outputs as **Parquet** or **Iceberg** tables
- Version control for data transformations
- Built-in data quality tests

**2. MinIO Container**
- S3-compatible object storage
- Store all data layers (Bronze, Silver, Gold)
- Lightweight alternative to AWS S3 for local/on-prem deployments
- Supports versioning and lifecycle policies

**3. Trino Container** (`trinodb/trino`)
- Distributed SQL query engine
- Query data directly from MinIO (S3)
- Supports multiple data formats (Parquet, Iceberg, ORC)
- Enables federated queries across multiple data sources
- High-performance analytics without moving data

#### Workflow

1. **Ingest**: Python pipeline writes Bronze layer to MinIO
2. **Transform**: dbt reads from MinIO, transforms data, writes Silver/Gold layers back to MinIO as Parquet/Iceberg
3. **Query**: Trino connects to MinIO, enables SQL queries on all layers
4. **Analyze**: Data analysts use Trino SQL interface or BI tools (Tableau, Metabase, Superset) connected to Trino

This architecture provides a production-grade, cloud-native data lakehouse that can run locally or in any cloud environment.
