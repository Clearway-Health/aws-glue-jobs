import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from datetime import datetime
import re
import os

import boto3
from botocore.exceptions import ClientError
import pandas as pd
from io import StringIO
from awsglue.dynamicframe import DynamicFrame
import urllib.parse

# Initialize SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Add the JDBC driver
jdbc_driver_path = "s3://cwh-glue-assets-prd-030063318327-s3b/Supported_Jars/mssql-jdbc-8.4.1.jre8.jar"
sc.addPyFile(jdbc_driver_path)

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'TempDir', 'input_path'])

def get_secret():
    secret_name = "RDS_Glue_ETL_User"
    region_name = "us-east-1"
    
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        print(f"Successfully retrieved secret: {secret_name}")
    except ClientError as e:
        print(f"Error fetching secret: {e}")
        raise e
    
    secret = eval(get_secret_value_response['SecretString'])
    return secret

s3_client = boto3.client('s3')

input_file_path = args['input_path']
print(f"Received input_path: {input_file_path}")

# Parse the S3 URL correctly, handling spaces
parsed_url = urllib.parse.urlparse(input_file_path)
input_bucket = parsed_url.netloc
input_file_key = urllib.parse.unquote(parsed_url.path.lstrip('/'))

print(f"Extracted bucket: {input_bucket}")
print(f"Extracted file key: {input_file_key}")

temp_folder = 'DecryptedFiles/temp_folder/'
transformed_file_key = f'{temp_folder}transformed_file.csv'

try:
    response = s3_client.get_object(Bucket=input_bucket, Key=input_file_key)
    file_content = response['Body'].read().decode('utf-8')
    print(f"File content (first 500 chars): {file_content[:500]}")
except ClientError as e:
    if e.response['Error']['Code'] == "NoSuchKey":
        print(f"Error: The specified key '{input_file_key}' does not exist in the bucket '{input_bucket}'.")
    else:
        print(f"Error fetching file from S3: {e}")
    raise e
    
file_name = os.path.basename(input_file_key)

df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)

df['Start Date'] = pd.to_datetime(df['Start Date']).dt.strftime('%m-%d-%Y')
df['End Date'] = pd.to_datetime(df['End Date']).dt.strftime('%m-%d-%Y')
df['Provider Last Name'] = df['Provider Name'].str.rsplit(n=1).str[0]
df['Provider First Name'] = df['Provider Name'].str.rsplit(n=1).str[1]

df['Load Timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
df['File Name'] = file_name

df = df[['Provider NPI','Provider Last Name','Provider First Name','Degree','Current Status','Department ID','Department Name',
         'Section Name', 'Start Date', 'End Date', 'Specialty', 'File Name', 'Load Timestamp']]
         
csv_buffer = StringIO()
df.to_csv(csv_buffer, sep=',', index=False, line_terminator='\n')
print(f"Transformed DataFrame head:\n{df.head(10)}")

try:
    response = s3_client.put_object(Bucket=input_bucket, Key=transformed_file_key, Body=csv_buffer.getvalue())
    print(f"Transformed file uploaded to S3: {response}")
except ClientError as e:
    print(f"Error uploading file to S3: {e}")
    raise e

s3_path = f's3://{input_bucket}/{transformed_file_key}'
spark_df = spark.read.option("header", True).csv(s3_path)

# Print Spark DataFrame info
print(f"Spark DataFrame schema: {spark_df.schema}")
print(f"Spark DataFrame count: {spark_df.count()}")

try:
    rds_credentials = get_secret()
    print(f"RDS credentials fetched successfully.")
except Exception as e:
    print(f"Error getting RDS credentials: {e}")
    raise e

username = rds_credentials['username']
password = rds_credentials['password']
hostname = rds_credentials['host']
database = "TBH"
port = rds_credentials['port']

print(f"Connecting to RDS: {hostname}:{port}")

# Construct JDBC URL
jdbc_url = f"jdbc:sqlserver://{hostname}:{port};databaseName={database};encrypt=true;trustServerCertificate=true;"

# Truncate the table before writing new data
def truncate_table(jdbc_url, table, username, password):
    try:
        connection = spark._sc._jvm.java.sql.DriverManager.getConnection(jdbc_url, username, password)
        statement = connection.createStatement()
        statement.execute(f"TRUNCATE TABLE {table}")
        print(f"Successfully truncated table: {table}")
        statement.close()
        connection.close()
    except Exception as e:
        print(f"Error truncating table: {str(e)}")
        raise e

truncate_table(jdbc_url, "dbo.TBH_Onco_Provider", username, password)

# Convert Spark DataFrame to DynamicFrame
dynamic_frame = DynamicFrame.fromDF(spark_df, glueContext, "dynamic_frame")

# Write to RDS using Glue dynamic frame
try:
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="cwh-glue-rds-sqlserver-connection",
        connection_options={
            "dbtable": "dbo.TBH_Onco_Provider",
            "database": database,
            "user": username,
            "password": password
        },
        transformation_ctx="datasink"
    )
    print("File successfully processed and written to RDS using Glue connection")
except Exception as e:
    print(f"Error occurred while writing to RDS using Glue connection: {str(e)}")
    print(f"Error type: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    
    # Fallback to direct JDBC connection
    print("Attempting to write using direct JDBC connection...")
    try:
        spark_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "dbo.TBH_Onco_Provider") \
            .option("user", username) \
            .option("password", password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .mode("overwrite") \
            .save()
        print("File successfully processed and written to RDS using direct JDBC connection")
    except Exception as e2:
        print(f"Error occurred while writing to RDS using direct JDBC connection: {str(e2)}")
        print(f"Error type: {type(e2).__name__}")
        traceback.print_exc()
        raise e2

# Verify data was written
try:
    verify_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", "dbo.TBH_Onco_Provider") \
        .option("user", username) \
        .option("password", password) \
        .load()

    print(f"Rows in database table: {verify_df.count()}")
except Exception as e:
    print(f"Error occurred while verifying data in RDS: {e}")
    import traceback
    traceback.print_exc()

# Optionally, delete the transformed file from S3
try:
    s3_client.delete_object(Bucket=input_bucket, Key=transformed_file_key)
    print(f"Transformed file deleted from S3: {transformed_file_key}")
except ClientError as e:
    print(f"Error deleting file from S3: {e}")

# Stop Spark session
spark.stop()

# Commit the job
job.commit()