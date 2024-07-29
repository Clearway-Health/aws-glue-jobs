import boto3
import logging
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Received event: %s", event)
    
    sns_client = boto3.client('sns', region_name='us-east-1')
    sns_topic_arn = 'arn:aws:sns:us-east-1:030063318327:Onco_Clearway_Appoint_Notifications'

    
    try:
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
        
        logger.info("Processing file: s3://%s/%s", s3_bucket, s3_key)
        
        if s3_key.startswith('DecryptedFiles/TBH-CWH-TFS/NYCBS_ONCOEMR_Appoint'):
            glue_client = boto3.client('glue', region_name='us-east-1')
            try:
                response = glue_client.start_job_run(
                    JobName='NYCBS_ONCOEMR_Appoint_Glue_Job',
                    Arguments={
                        '--input_path': f's3://{s3_bucket}/{s3_key}'
                    }
                )
                job_run_id = response['JobRunId']
                logger.info("Glue job started: %s", job_run_id)
                
                # Poll for job completion
                while True:
                    job_status = glue_client.get_job_run(JobName='NYCBS_ONCOEMR_Appoint_Glue_Job', RunId=job_run_id)
                    status = job_status['JobRun']['JobRunState']
                    logger.info("Job status: %s", status)
                    
                    if status in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                        break
                    time.sleep(30)  # Wait for 30 seconds before checking the status again
                
                if status == 'SUCCEEDED':
                    logger.info(f'Glue job NYCBS_ONCOEMR_Appoint_Glue_Job completed successfully for file {s3_key}.')
                    sns_message = f'Glue job NYCBS_ONCOEMR_Appoint_Glue_Job completed successfully for file {s3_key}.'
                    sns_client.publish(
                        TopicArn=sns_topic_arn,
                        Subject='Glue Job Succeeded',
                        Message=sns_message
                    )
                else:
                    error_message = job_status['JobRun'].get('ErrorMessage', 'No error message provided')
                    logger.error(f'Glue job NYCBS_ONCOEMR_Appoint_Glue_Job failed for file {s3_key}. Status: {status}. Error: {error_message}')
                    sns_message = f'Glue job NYCBS_ONCOEMR_Appoint_Glue_Job failed for file {s3_key}. Status: {status}. Error: {error_message}'
                    sns_client.publish(
                        TopicArn=sns_topic_arn,
                        Subject='Glue Job Failed for NYCBS_ONCOEMR_Appoint*',
                        Message=sns_message
                    )
                return {
                    'statusCode': 200,
                    'body': f'Job {status}'
                }
            except glue_client.exceptions.ConcurrentRunsExceededException as e:
                logger.error("ConcurrentRunsExceededException: %s", e)
                sns_message = f"ConcurrentRunsExceededException: {e}"
                sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Subject='Glue Job Error',
                    Message=sns_message
                )
                return {
                    'statusCode': 429,
                    'body': f"ConcurrentRunsExceededException: {e}"
                }
            except glue_client.exceptions.EntityNotFoundException as e:
                logger.error("EntityNotFoundException: %s", e)
                sns_message = f"EntityNotFoundException: {e}"
                sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Subject='Glue Job Error',
                    Message=sns_message
                )
                return {
                    'statusCode': 404,
                    'body': f"EntityNotFoundException: {e}"
                }
            except glue_client.exceptions.InvalidInputException as e:
                logger.error("InvalidInputException: %s", e)
                sns_message = f"InvalidInputException: {e}"
                sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Subject='Glue Job Error',
                    Message=sns_message
                )
                return {
                    'statusCode': 400,
                    'body': f"InvalidInputException: {e}"
                }
            except Exception as e:
                logger.error("Exception: %s", e)
                sns_message = f"Exception: {e}"
                sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Subject='Glue Job Error',
                    Message=sns_message
                )
                return {
                    'statusCode': 500,
                    'body': f"Exception: {e}"
                }
        else:
            logger.info("File does not match pattern, no action taken.")
            return {
                'statusCode': 200,
                'body': 'File does not match pattern, no action taken.'
            }
    except KeyError as e:
        logger.error("KeyError: %s", e)
        sns_message = f"KeyError: {e}"
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject='Lambda Function Error for NYCBS_ONCOEMR_Appoint*',
            Message=sns_message
        )
        return {
            'statusCode': 400,
            'body': f"KeyError: {e}"
        }
    except Exception as e:
        logger.error("Exception: %s", e)
        sns_message = f"Exception: {e}"
        sns_client.publish(
            TopicArn=sns_topic_arn,
            Subject='Lambda Function Error NYCBS_ONCOEMR_Appoint*',
            Message=sns_message
        )
        return {
            'statusCode': 500,
            'body': f"Exception: {e}"
        }
