AWS Data Lake ETL Pipeline (Telemetry Data)
📌 Project Overview

This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline built using AWS Glue and PySpark. The pipeline processes semi-structured IoT telemetry data stored in Amazon S3, transforms it into a clean and structured format, and enables fast analytical querying using Amazon Athena.

🧰 Tech Stack
AWS S3 (Data Lake Storage)
AWS Glue (ETL using PySpark)
Amazon Athena (Serverless Query Engine)
Python (PySpark)
⚙️ Pipeline Architecture
Raw JSON telemetry data is stored in Amazon S3 (raw layer)
AWS Glue job reads the data using DynamicFrames
Data is transformed using PySpark:
Schema mapping
Timestamp conversion
Data cleaning
Data is converted into Parquet format
Data is partitioned by event_date and event_type
Processed data is stored in S3 (curated layer)
Amazon Athena is used to query the data
🔄 Data Transformations
Flattened nested JSON fields (payload.temperature, payload.status)
Converted event_time from string to timestamp format
Created event_date column for partitioning
Removed invalid/null records for data quality
Applied partitioning for optimized query performance
📊 Athena Query Example
🔍 Sample Query
SELECT * 
FROM telemetry_clean 
WHERE event_type = 'sensor_reading';

🎯 Key Learnings
Built an end-to-end data pipeline using AWS Glue and PySpark
Understood data lake architecture (raw → curated layers)
Improved query performance using partitioning
Used Parquet format for efficient storage and analytics
Gained hands-on experience with AWS services (S3, Glue, Athena)


