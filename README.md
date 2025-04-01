# MovieLens Streaming Data Pipeline

A complete end-to-end streaming data pipeline for the MovieLens 20M dataset using modern data engineering tools.

## Architecture

![Architecture Diagram](docs/architecture.png)

## Technologies Used

- **Infrastructure as Code**: Terraform
- **Event Streaming**: Apache Kafka (KRaft)
- **Data Processing**: DLTHub with Spark/Flink Streaming
- **Data Storage**: S3/Iceberg Lakehouse (Medallion Architecture)
- **Transformations**: dbt-core with PySpark
- **Workflow Orchestration**: Kestra
- **Visualization**: Metabase

## Project Structure

- `infra/`: Terraform scripts for infrastructure
- `scripts/`: Utility scripts
- `datasets/`: Dataset handling scripts
- `dbt_project/`: dbt models
- `notebooks/`: Jupyter notebooks for exploration

## Getting Started

[Instructions to be added]