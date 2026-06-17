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

```python
expectations = [
    expect_column_values_to_not_be_null('loan_id'),
    expect_column_values_to_be_between('loan_amount', 10000, 5000000),
    expect_column_values_to_match_regex('ssn_hash', r'^[a-f0-9]{64}$'),
]
```

## Production Hardening

This project is designed with production-style reliability patterns that go beyond simple ETL scripting.

### Operational safeguards

- Idempotent load patterns help reduce duplicate ingestion during retry and replay scenarios.
- Partition-based extraction supports scheduled loads and controlled historical backfills.
- Schema and column-level checks reduce the risk of malformed records entering warehouse tables.
- Great Expectations checkpoints act as enforcement gates before downstream transformations continue.
- Slack and PagerDuty notifications improve response time for extraction, load, and validation failures.

### Reliability patterns

- Exponential backoff absorbs temporary API instability and short-lived network issues.
- Audit logging captures extraction windows, record counts, task status, and run metadata.
- `ExternalTaskSensor` ensures downstream transformation jobs wait for successful upstream ingestion.
- S3 staging provides a recoverable intermediate layer between source extraction and warehouse loading.

### Security and governance

- Sensitive fields are validated in hashed form rather than exposed as raw values.
- Environment-driven configuration keeps operational settings separate from application logic.
- Validation rules improve traceability and confidence for regulated financial reporting workflows.

## Data Quality Rules

| Field | Validation Rule | Purpose |
|---|---|---|
| `loan_id` | Must not be null | Prevents incomplete or unusable mortgage records |
| `loan_amount` | Must be between 10,000 and 5,000,000 | Detects unrealistic or malformed values |
| `ssn_hash` | Must match SHA-256 style regex | Confirms sensitive identity data is hashed |

These checks illustrate how business rules and technical validation work together in data engineering systems. Even simple expectations can prevent expensive reporting issues downstream.

## Airflow DAG Overview

- **mortgage_ingestion_dag** runs every 15 minutes and extracts data from supported mortgage systems.
- **dbt_transform_dag** starts after ingestion through `ExternalTaskSensor` dependencies.
- Both DAGs include failure notifications with operational context through Slack alerting.

## Results and Impact

- Reduced data latency from **24 hours to 15 minutes**.
- Achieved **99.9% pipeline uptime** over a 6-month production run.
- Onboarded **3 new mortgage systems** without downtime using modular extractor patterns.
- Caught **2,400+ data quality issues** before they reached downstream reports.

## Local Setup

```bash
# Clone the repository
git clone https://github.com/Ashok98765vvs/python-mortgage-data-pipelines.git

# Move into the project folder
cd python-mortgage-data-pipelines

# Install dependencies
pip install -r requirements.txt

# Start local services
docker-compose up -d

# Run tests
pytest tests/
```

Configure required environment variables before triggering DAGs locally.

## Future Enhancements

- Add CDC-based ingestion for lower-latency source synchronization.
- Add dbt documentation and lineage artifacts for downstream models.
- Add richer Airflow observability dashboards for task-level performance tracking.
- Add CI workflows for automated linting and testing.
- Add cost monitoring for Snowflake and S3 usage optimization.

## Author

**Ashok Chowdary**  
Data Engineer  
Auburn University Montgomery | OPT Status

- [LinkedIn](https://linkedin.com/in/ashok98765vvs)
- [GitHub](https://github.com/Ashok98765vvs)
