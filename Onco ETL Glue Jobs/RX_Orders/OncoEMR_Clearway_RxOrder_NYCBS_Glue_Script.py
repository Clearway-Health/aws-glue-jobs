import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, to_date

# Retrieve job arguments
args = getResolvedOptions(sys.argv, ['input_path'])

# Initialize Spark session
spark = SparkSession.builder.appName('GlueTransformJob').getOrCreate()

# Read CSV file from S3
input_path = args['input_path']
df = spark.read.csv(input_path, header=True)

# Transformation: Remove non-numeric characters from Patient_Phone_Number
df = df.withColumn('Patient_phone_number', regexp_replace(col('Patient_Phone_Number'), '[^0-9]', ''))

# Transformation: Convert Date_Sent to date format without time
df = df.withColumn('Date_Sent', to_date(col('Date_Sent')))

# Function to get RDS credentials from Secrets Manager
def get_secret():
    secret_name = "rds/credentials"
    region_name = "us-west-2"  # Update with your AWS region if different
    
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

# Write the transformed data to RDS
df.write \
    .format("jdbc") \
    .option("url", f"jdbc:sqlserver://{hostname}:1433;databaseName={database}") \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("dbtable", "TBH_Onco_RxOrders") \
    .option("user", username) \
    .option("password", password) \
    .mode("append") \
    .save()
