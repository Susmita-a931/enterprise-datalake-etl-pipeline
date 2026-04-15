import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, to_timestamp, to_date

# Configure basic logging for CloudWatch
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Initializing Glue ETL Job for Telemetry Data")
    
    # Retrieve arguments passed at runtime
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'source_s3_path', 'target_s3_path'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    source_path = args['source_s3_path']
    target_path = args['target_s3_path']

    try:
        logger.info(f"Extracting raw telemetry data connecting to source: {source_path}")
        
        # Ingest raw JSON logs
        raw_dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [source_path], "recurse": True},
            format="json",
            transformation_ctx="raw_dynamic_frame"
        )

        logger.info("Applying schema mapping and flattening payload")
        
        # Flatten payload and rename fields for downstream analytics
        mapped_frame = ApplyMapping.apply(
            frame=raw_dynamic_frame,
            mappings=[
                ("device_id", "string", "device_id", "string"),
                ("event_time", "string", "event_time", "string"),
                ("event_type", "string", "event_type", "string"),
                ("payload.temperature", "double", "temperature", "double"),
                ("payload.status", "string", "device_status", "string")
            ],
            transformation_ctx="mapped_frame"
        )

        logger.info("Converting to Spark DataFrame for complex timestamp parsing and deduplication")
        
        spark_df = mapped_frame.toDF()
        
        # Cast timestamps and extract event_date for partitioning strategy
        spark_df = spark_df.withColumn(
            "event_time",
            to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX")
        )
        spark_df = spark_df.withColumn("event_date", to_date(col("event_time")))
        
        # Business Rule: Drop dirty records missing critical identifiers
        initial_count = spark_df.count()
        cleaned_df = spark_df.dropna(subset=['device_id', 'event_time'])
        final_count = cleaned_df.count()
        logger.info(f"Data validation complete. Dropped {initial_count - final_count} invalid records.")

        # Convert back to DynamicFrame for writing
        cleaned_dynamic_frame = DynamicFrame.fromDF(cleaned_df, glueContext, "cleaned_dynamic_frame")

        logger.info(f"Writing curated data to target: {target_path}")
        
        # Sink to S3 as partitioned Parquet optimized for Athena
        glueContext.write_dynamic_frame.from_options(
            frame=cleaned_dynamic_frame,
            connection_type="s3",
            connection_options={
                "path": target_path,
                "partitionKeys": ["event_date", "event_type"]
            },
            format="parquet",
            transformation_ctx="data_sink_parquet"
        )

        job.commit()
        logger.info("Glue Job completed successfully.")

    except Exception as e:
        logger.error(f"ETL Job failed due to an error: {str(e)}", exc_info=True)
        raise e

if __name__ == '__main__':
    main()
