# ****************************************************************************
# Copyright © 2024 LeapLogic.
#
# This is a copyrighted work.
# This program and the information contained in it is confidential and proprietary 
# to Clearway Health.
# You are prohibited from making a copy or modification of, or from redistributing,
# reproducing, rebroadcasting, or re-encoding of this content without the prior 
# written consent of Clearway Health. 
# ****************************************************************************

import boto3
import pandas as pd
from io import StringIO
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession

# Function to get RDS credentials from Secrets Manager
def get_secret():
    secret_name = "rds/credentials"
    region_name = "us-east-1"  # N. Virginia
    
    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise e
    
    # Parse the secret response
    secret = eval(get_secret_value_response['SecretString'])
    return secret

# Fetch RDS credentials
rds_credentials = get_secret()
username = rds_credentials['username']
password = rds_credentials['password']
hostname = 'clearwayhealth-prd-db-drzd6edlqf2d.cfyv5tdjuovq.us-east-1.rds.amazonaws.com'
database = 'TBH'

# Initialize the S3 client
s3_client = boto3.client('s3')

# Define the input S3 bucket and file paths
input_bucket = 'bmc-tfs-bucket-030063318327'
input_file_key = 'DecryptedFiles/TBH-CWH-TFS/NYCBS_ONCOEMR_Appoint_20240701_ 74630.csv'

try:
    # Read the file from S3
    response = s3_client.get_object(Bucket=input_bucket, Key=input_file_key)
    file_content = response['Body'].read().decode('utf-8')

    # Load the file into a pandas DataFrame using the specified delimiter
    df = pd.read_csv(StringIO(file_content), delimiter='|', dtype=str)

    # Replace NaN with empty string to ensure the correct format
    df = df.fillna('')

    # Format the date columns
    # Convert 'Appointment Time' to HH:MM format
    df['Appointment Time'] = pd.to_datetime(df['Appointment Time'], format='%H:%M:%S').dt.strftime('%H:%M')

    # Convert 'Appointment Date' to MM-DD-YYYY format
    df['Appointment Date'] = pd.to_datetime(df['Appointment Date'], format='%Y-%m-%d').dt.strftime('%m-%d-%Y')

    # Save the processed DataFrame to a CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, sep=',')

    # Initialize Spark session
    spark = SparkSession.builder.appName("onco_Appoint").getOrCreate()

    # Convert pandas DataFrame to Spark DataFrame
    spark_df = spark.read.option("header", True).csv(StringIO(csv_buffer.getvalue()))

    # Write the transformed data to RDS
    spark_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:sqlserver://{hostname}:1433;databaseName={database}") \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", "dbo.[TBH_Onco_Appointments]") \
        .option("user", username) \
        .option("password", password) \
        .mode("append") \
        .save()

    print("File successfully processed and written to RDS")

except ClientError as e:
    print(f"Error processing the file: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
