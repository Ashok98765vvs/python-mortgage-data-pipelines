# Python Mortgage Data Pipelines

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Great Expectations](https://img.shields.io/badge/Great%20Expectations-FF6B35?style=for-the-badge)

## Overview
Enterprise-grade Python data ingestion pipelines connecting **15+ proprietary mortgage systems** to a Snowflake data warehouse. Built with Apache Airflow for orchestration, Great Expectations for data quality, and custom retry/alerting logic for production reliability.

## Key Highlights
- Ingests data from **15+ proprietary mortgage systems** (LOS, servicing, payment processors)
- Custom Python extractors with **exponential backoff retry** (30s → 60s → 120s)
- Loads to Snowflake via optimized **S3 staging + COPY INTO** pattern
- Data quality validation using **Great Expectations** with Slack + PagerDuty alerting
- Airflow DAGs with **ExternalTaskSensor** for cross-pipeline dependencies
- Full audit logging and pipeline observability
- Reduced data latency from 24 hours to **near real-time** (15-minute SLA)

## Tech Stack
| Technology | Purpose |
|-----------|----------|
| Python 3.10 | Core pipeline logic, extractors, transformers |
| Apache Airflow 2.7 | Workflow orchestration and scheduling |
| Snowflake | Cloud data warehouse target |
| AWS S3 | Intermediate staging layer |
| Great Expectations | Data quality validation framework |
| PagerDuty + Slack | Alerting and incident management |
| Docker | Containerized Airflow deployment |

## Project Structure
```
python-mortgage-data-pipelines/
├── ingestion/
│   ├── extractors/
│   │   ├── base_extractor.py        # Abstract base with retry logic
│   │   ├── los_extractor.py         # Loan Origination System extractor
│   │   ├── payment_extractor.py     # Payment processor extractor
│   │   └── servicing_extractor.py   # Loan servicing system extractor
│   └── loaders/
│       ├── s3_loader.py             # S3 staging upload
│       └── snowflake_loader.py      # Snowflake COPY INTO loader
├── dags/
│   ├── mortgage_ingestion_dag.py    # Main ingestion DAG
│   └── dbt_transform_dag.py        # dbt transformation trigger DAG
├── observability/
│   ├── great_expectations/          # GE checkpoints and suites
│   ├── alerting.py                  # Slack + PagerDuty alert helpers
│   └── audit_logger.py              # Pipeline audit trail
├── utils/
│   ├── retry.py                     # Exponential backoff decorator
│   └── config.py                    # Environment configuration
├── tests/
│   ├── test_extractors.py
│   └── test_loaders.py
├── requirements.txt
└── docker-compose.yml
```

## Pipeline Architecture
```
Mortgage Systems (15+)
        |
        v
Python Extractors (retry + pagination + audit)
        |
        v
AWS S3 Staging Bucket
        |
        v
Snowflake COPY INTO (bulk load)
        |
        v
Great Expectations Validation
        |
   Pass / Fail
    /        \
  dbt        Slack/PagerDuty Alert
```

## Core Features

### Exponential Backoff Retry
```python
@retry(max_attempts=3, backoff_seconds=[30, 60, 120])
def extract_loan_data(system_id: str, date: str) -> pd.DataFrame:
    response = mortgage_api.get_loans(system_id=system_id, date=date)
    return response.to_dataframe()
```

### Snowflake S3 COPY INTO
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

### Great Expectations Data Quality
```python
expectations = [
    expect_column_values_to_not_be_null('loan_id'),
    expect_column_values_to_be_between('loan_amount', 10000, 5000000),
    expect_column_values_to_match_regex('ssn_hash', r'^[a-f0-9]{64}$'),
]
```

## Airflow DAG Overview
- **mortgage_ingestion_dag**: Runs every 15 minutes, extracts from all 15+ systems
- **dbt_transform_dag**: Triggered after ingestion via ExternalTaskSensor, runs dbt models
- Both DAGs include Slack notifications on failure with full error context

## Results & Impact
- Reduced data latency from **24 hours to 15 minutes**
- Achieved **99.9% pipeline uptime** over 6 months production run
- Onboarded **3 new mortgage systems** without downtime using modular extractor pattern
- Caught **2,400+ data quality issues** before they reached downstream reports

## Author
**Ashok Chowdary** | Data Engineer  
Auburn University Montgomery | OPT Status  
[LinkedIn](https://linkedin.com/in/ashok98765vvs) | [GitHub](https://github.com/Ashok98765vvs)
