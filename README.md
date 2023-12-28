Overview
This project involves the development and execution of AWS Glue ETL scripts and SQL queries for data processing and transformation. The objective is to curate, filter, and store data from different sources in Amazon S3 buckets. The curated data is then used to create external tables in AWS Athena for further analysis.

Contents
File 1 `accelerometer_landing_to_trusted.py`: ETL Script for Accelerometer Trusted
File 2 `customer_landing_to_trusted.py`: ETL Script for Customer Trusted
File 3 `customer_trusted_to_curated.py`: ETL Script for Customer Curated
File 4 `machine_learning_curated.py`: ETL Script for Machine Learning Curated
File 5 `step_trainer_to_trusted.py`: ETL Script for Step Trainer Trusted
File 6 `accelerometer_landing.sql`: Athena SQL Table Definition for acceleromater landing
File 7 `customer_landing.sql`: Athena SQL Table Definition for acceleromater landing

<a name="file-1"></a>File 1: ETL Script for Accelerometer Trusted
Purpose: This script processes data from the "accelerometer_landing" and "customer_trusted" tables, performs a join operation, drops unnecessary fields, and writes the resulting data to the "s3://adadoun-stedi-lake-house/accelerometer_trusted/" S3 path.
Execution:
bash
Copy code
python script1.py
<a name="file-2"></a>File 2: ETL Script for Customer Trusted
Purpose: This script processes data from the "customer_landing" table, applies a privacy filter, and writes the resulting data to the "s3://adadoun-stedi-lake-house/customer_trusted/" S3 path.
Execution:
bash
Copy code
python script2.py
<a name="file-3"></a>File 3: ETL Script for Accelerometer Landing
Purpose: This script processes data from the "accelerometer_landing" table and writes the resulting data to the "s3://adadoun-stedi-lake-house/accelerometer_landing/" S3 path.
Execution:
bash
Copy code
python script3.py
<a name="file-4"></a>File 4: ETL Script for Accelerometer Trusted and Customer Trusted Join
Purpose: This script performs a join operation between "accelerometer_trusted" and "customer_trusted," drops unnecessary fields, and writes the resulting data to the "s3://adadoun-stedi-lake-house/customer_curated/" S3 path.
Execution:
bash
Copy code
python script4.py
<a name="file-5"></a>File 5: ETL Script for Machine Learning Curated
Purpose: This script performs a SQL query join between "step_trainer_trusted" and "accelerometer_trusted," and writes the resulting data to the "s3://adadoun-stedi-lake-house/machine_learning_curated/" S3 path.
Execution:
bash
Copy code
python script5.py
<a name="file-6"></a>File 6: ETL Script for Customer Curated
Purpose: This script performs a SQL query join between "step_trainer_landing" and "customer_curated," drops duplicates, and writes the resulting data to the "s3://adadoun-stedi-lake-house/step_trainer_trusted/" S3 path.
Execution:
bash
Copy code
python script6.py
<a name="file-7"></a>File 7: ETL Script for Step Trainer Trusted
Purpose: This script performs a SQL query join between "step_trainer_landing" and "customer_curated," drops duplicates, and writes the resulting data to the "s3://adadoun-stedi-lake-house/step_trainer_trusted/" S3 path.
Execution:
bash
Copy code
python script7.py

<a name="file-8"></a>File 8: Athena SQL Table Definition
Purpose: This SQL script defines an external table in Athena named "customer_landing" with its schema and location based on the "s3://adadoun-stedi-lake-house/customer_landing/" S3 path.
Execution and Configuration
Make sure you have AWS Glue configured with the necessary permissions.
Ensure that the required libraries and dependencies are installed.
Execute the scripts individually based on your data processing needs.