from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# Strict production level pipeline SLA arguments
default_args = {
    'owner': 'ranjith_kumaran',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email': ['ranjithkumaranvm374@gmail.com'],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dep_core_data_pipeline_orchestration',
    default_args=default_args,
    description='Production batch orchestration framework for daily S3 to Parquet conversion',
    schedule_interval='30 2 * * *',  # Runs dynamically every night at 2:30 AM
    catchup=False,
    tags=['production', 'data_lake', 'aws_glue']
) as dag:

    # Task 1: Trigger the custom Glue job written above
    trigger_glue_job = GlueJobOperator(
        task_id='execute_glue_spark_transformations',
        job_name='glue_ingestion_spark_pipeline',
        aws_conn_id='aws_production_credentials',
        region_name='us-east-1'
    )

    # Task 2: Sync partitions in Athena so reporting dashboards update instantly
    refresh_athena_metadata = AthenaOperator(
        task_id='sync_athena_lakehouse_partitions',
        query='MSCK REPAIR TABLE dep_warehouse.user_activity_reporting;',
        database='dep_warehouse',
        output_location='s3://dep-athena-metadata-logs/query_history/',
        aws_conn_id='aws_production_credentials'
    )

    # Pipeline structural flow
    trigger_glue_job >> refresh_athena_metadata
