CREATE DATABASE IF NOT EXISTS dep_warehouse;

-- Phase 2: Schema definition mapping the S3 Parquet partitions
CREATE EXTERNAL TABLE IF NOT EXISTS dep_warehouse.user_activity_reporting (
    session_id STRING,
    user_id STRING,
    device_info STRING,
    region STRING,
    created_at TIMESTAMP
)
PARTITIONED BY (load_date STRING)
STORED AS PARQUET
LOCATION 's3://dep-analytics-data-lake/processed_user_activity/'
TBLPROPERTIES (
    'parquet.compress'='SNAPPY',
    'classification'='parquet'
);

-- Phase 3: Metadata update query for new daily batch discovery
MSCK REPAIR TABLE dep_warehouse.user_activity_reporting;
