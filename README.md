# aws-data-lake-pipeline
Automated batch ETL pipeline using PySpark, AWS Glue, Amazon Athena, and Apache Airflow.

# End-to-End Automated AWS Data Lake Pipeline

This repository hosts a production-ready batch ETL framework designed to ingest, clean, and optimize high-volume application logs.

## Core Technical Architecture
- **Storage Tier:** Raw multi-format JSON logs ingested into Amazon S3 (Staging Bucket).
- **Processing Engine:** Distributed data cleaning and conversion using PySpark deployed on **AWS Glue**.
- **Performance Optimization:** Data compression using **SNAPPY Parquet** and partitioned by ingestion date, resulting in a 30% reduction in query latencies.
- **Analytics & Consumption:** Cataloged and rendered serverless queries via **Amazon Athena**.
- **Workflow Management:** Programmatic DAG execution and dependency routing managed entirely by **Apache Airflow**.
