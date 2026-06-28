"""Pipeline configuration loaded from environment variables."""
import os
from dataclasses import dataclass, field


@dataclass
class SnowflakeConfig:
    account: str
    user: str
    password: str
    database: str
    schema: str
    warehouse: str
    role: str

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            database=os.environ.get("SNOWFLAKE_DATABASE", "MORTGAGE_DB"),
            schema=os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            role=os.environ.get("SNOWFLAKE_ROLE", "DATA_ENGINEER"),
        )


@dataclass
class S3Config:
    bucket: str
    prefix: str
    region: str

    @classmethod
    def from_env(cls) -> "S3Config":
        return cls(
            bucket=os.environ["S3_STAGING_BUCKET"],
            prefix=os.environ.get("S3_PREFIX", "mortgage-pipeline/staging/"),
            region=os.environ.get("AWS_REGION", "us-east-1"),
        )


@dataclass
class PipelineConfig:
    snowflake: SnowflakeConfig = field(default_factory=SnowflakeConfig.from_env)
    s3: S3Config = field(default_factory=S3Config.from_env)
    batch_size: int = int(os.environ.get("PIPELINE_BATCH_SIZE", 500))
    max_retries: int = int(os.environ.get("PIPELINE_MAX_RETRIES", 3))
    retry_delay_seconds: int = int(os.environ.get("PIPELINE_RETRY_DELAY", 30))
