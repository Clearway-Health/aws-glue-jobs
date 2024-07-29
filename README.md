# Glue/Lambda Pipeline with CloudWatch Events and SNS Notifications

This guide explains the steps to set up an AWS Glue job triggered by an S3 event, monitored by CloudWatch Events, and sends notifications via SNS when the job state changes.

## Glue Scripts

1. The script begins by setting up necessary configurations, including importing libraries and defining functions.
2. It retrieves database credentials securely from AWS Secrets Manager.
3. The script reads an input file from an Amazon S3 bucket. This file contains data that needs to be processed and stored.
4. The cleaned data is converted into a Spark DataFrame.
5. Finally, the transformed data is written to an Amazon RDS database. This involves connecting to the database and appending the new data to a specific table.


## Lambda Functions
The Lambda function is designed to automatically trigger a Glue job when a specific file is uploaded to an S3 bucket. It ensures that the Glue job runs successfully and notifies the appropriate parties via SNS in case of success or failure. 


## Conclusion
The combination of AWS Glue and AWS Lambda creates an automated data processing pipeline. Lambda is automatically triggers the Glue job, ensuring that data is processed as soon as it is available. Lambda monitors the Glue job and sends real-time notifications, ensuring that any issues are promptly communicated.

