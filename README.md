# Glue/Lambda Pipeline with CloudWatch Events and SNS Notifications

This guide explains the steps to set up an AWS Glue job triggered by an S3 event, monitored by CloudWatch Events, and sends notifications via SNS when the job state changes.

## Prerequisites

### AWS Services Required
- AWS Glue
- AWS Lambda
- Amazon S3
- Amazon RDS
- AWS Secrets Manager
- Amazon SNS

## IAM Roles and Permissions
### Glue Specific Permissions:
- glue:CreateJob
- glue:StartJobRun
- glue:GetJob
- glue:GetJobRun
- glue:GetJobRuns
- glue:BatchGetJobs
- glue:BatchGetPartition
- glue:BatchGetTable
  
#### S3 Permissions:
- s3:GetObject
- s3:ListBucket
- s3:PutObject

#### Secrets Manager Permissions:
- secretsmanager:GetSecretValue
- secretsmanager:DescribeSecret
- RDS Permissions (if needed for other operations beyond database access via JDBC):

### Lambda Specific Permissions:

- lambda:InvokeFunction
- lambda:CreateFunction
- lambda:UpdateFunctionCode
- lambda:GetFunction
- lambda:UpdateFunctionConfiguration
- lambda:CreateEventSourceMapping
- lambda:DeleteEventSourceMapping

### S3 Permissions:
- s3:GetObject
- s3:ListBucket
- s3:PutObject
- s3:GetBucketNotification
- s3:PutBucketNotification

### Glue Permissions:
- glue:StartJobRun
- glue:GetJobRun
- glue:GetJobRuns
- glue:GetJob
- glue:BatchGetJobs
- glue:GetJobs

### SNS Permissions:
- sns:Publish

### CloudWatch Logs Permissions (for logging):
- logs:CreateLogGroup
- logs:CreateLogStream
- logs:PutLogEvents
- logs:DescribeLogStreams

### CloudWatch Permissions:
- cloudwatch:PutMetricData
- cloudwatch:GetMetricData
- cloudwatch:ListMetrics

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

