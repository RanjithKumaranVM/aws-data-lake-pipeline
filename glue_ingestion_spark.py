import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, to_timestamp, upper, current_date

# Setup logging for production tracking
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("glue_logger")

# Fetch job arguments dynamically (Production Standard)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    logger.info("Starting Spark Ingestion pipeline from Staging S3 Bucket...")
    
    # Ingesting raw application JSON logs
    source_uri = "s3://dep-staging-data-zone/raw_application_logs/"
    df_raw = spark.read.json(source_uri)
    
    logger.info(f"Successfully loaded raw records. Schema validation in progress...")
    
    # Production-grade Data Cleaning & Column Standardization
    df_transformed = df_raw.filter(col("session_id").isNotNull()) \
                           .withColumn("created_at", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss")) \
                           .withColumn("region", upper(col("country"))) \
                           .withColumn("load_date", current_date()) \
                           .drop("country", "event_time")
                           
    # Target path optimized for Columnar Queries (Parquet format)
    target_uri = "s3://dep-analytics-data-lake/processed_user_activity/"
    
    logger.info("Writing optimized partitioned Parquet data back to S3 Data Lake...")
    
    df_transformed.write \
        .mode("append") \
        .partitionBy("load_date") \
        .parquet(target_uri)
        
    logger.info("ETL Spark Pipeline executed successfully without errors.")
    job.commit()

except Exception as error:
    logger.error(f"Critical Pipeline Failure in Glue Transformation: {str(error)}")
    raise error
