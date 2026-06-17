# Python Mortgage Data Pipelines

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-FF6B35?style=for-the-badge)

## Overview

Enterprise-grade Python data ingestion pipelines connecting **15+ proprietary mortgage systems** to a Snowflake data warehouse. Built with Apache Airflow for orchestration, Great Expectations for data quality, and custom retry, observability, and alerting logic for production reliability.

This project demonstrates how to design resilient data pipelines for regulated financial systems where freshness, trust, and operational visibility matter. It emphasizes modular extractors, S3-based staging, automated validation, and scalable warehouse loading patterns.

## Key Highlights

- Ingests data from **15+ proprietary mortgage systems** across LOS, servicing, and payment platforms.
- Uses custom Python extractors with **exponential backoff retry** for resilient data ingestion.
- Loads data into Snowflake through an optimized **S3 staging + COPY INTO** pattern.
- Applies **Great Expectations** checkpoints to prevent bad data from reaching downstream consumers.
- Uses Airflow DAG dependencies with **ExternalTaskSensor** for cross-pipeline coordination.
- Includes audit logging, Slack + PagerDuty alerting, and a **15-minute SLA** for freshness.

## Pipeline Architecture

```text
Mortgage Systems (15+)
        │
        ▼
Python Extractors
(retry + pagination + audit)
        │
        ▼
AWS S3 Staging Bucket
        │
        ▼
Snowflake COPY INTO
(bulk load)
        │
        ▼
Great Expectations Validation
     ┌──────────────┴──────────────┐
     ▼                             ▼
dbt / downstream models     Slack + PagerDuty alerts
```

This design separates extraction, staging, warehouse loading, and validation into distinct layers. That makes the platform easier to scale, test, observe, and extend when new mortgage systems are added.

## Tech Stack

| Technology | Purpose |
|---|---|
| Python 3.10 | Core pipeline logic, extractors, and utilities |
| Apache Airflow 2.7 | Workflow orchestration and scheduling |
| Snowflake | Cloud data warehouse target |
| AWS S3 | Intermediate staging layer |
| Great Expectations | Data quality validation framework |
| PagerDuty + Slack | Alerting and incident management |
| Docker | Containerized Airflow deployment |

## Project Structure

```text
python-mortgage-data-pipelines/
├── ingestion/
│   ├── extractors/
│   │   ├── base_extractor.py
│   │   ├── los_extractor.py
│   │   ├── payment_extractor.py
│   │   └── servicing_extractor.py
│   └── loaders/
│       ├── s3_loader.py
│       └── snowflake_loader.py
├── dags/
│   ├── mortgage_ingestion_dag.py
│   └── dbt_transform_dag.py
├── observability/
│   ├── great_expectations/
│   ├── alerting.py
│   └── audit_logger.py
├── utils/
│   ├── retry.py
│   └── config.py
├── tests/
│   ├── test_extractors.py
│   └── test_loaders.py
├── requirements.txt
└── docker-compose.yml
```

The structure is organized around clear separation of concerns: extraction, loading, orchestration, observability, and testing. This makes the project easier to maintain and easier to explain in interviews.

## Core Features

### Exponential Backoff Retry

The pipeline uses controlled retry logic to handle temporary API or network failures without immediately failing the workflow.

```python
@retry(max_attempts=3, backoff_seconds=)
def extract_loan_data(system_id: str, date: str) -> pd.DataFrame:
    response = mortgage_api.get_loans(system_id=system_id, date=date)
    return response.to_dataframe()
```

### Snowflake Bulk Load Pattern

Data is staged in S3 and loaded into Snowflake through `COPY INTO`, which supports scalable warehouse ingestion.

```python
def load_to_snowflake(s3_path: str, table: str, schema: str):
    cursor.execute(f"""
        COPY INTO {schema}.{table}
        FROM @{S3_STAGE}/{s3_path}
        FILE_FORMAT = (TYPE = PARQUET)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = SKIP_FILE
    """)
```

### Data Quality Validation

Great Expectations is used to validate critical mortgage fields before data moves downstream.

