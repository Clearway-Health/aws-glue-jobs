import boto3
import re
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.info("Received event: %s", event)
    
    try:
        s3_bucket = event['Records'][0]['s3']['bucket']['name']
        s3_key = event['Records'][0]['s3']['object']['key']
        
        logger.info("Processing file: s3://%s/%s", s3_bucket, s3_key)
        
        # Debugging: Print the s3_key
        logger.info("s3_key: %s", s3_key)
        
        # Check if the file name starts with the desired pattern
        if s3_key.startswith('DecryptedFiles/TBH-CWH-TFS/OncoEMR_Clearway_RxOrder'):
            client = boto3.client('glue')
            response = client.start_job_run(
                JobName='OncoEMR_Clearway_RxOrder_NYCBS',  # Ensure this matches the actual job name in Glue
                Arguments={
                    '--input_path': f's3://{s3_bucket}/{s3_key}'
                }
            )
            logger.info("Glue job started: %s", response)
            return {
                'statusCode': 200,
                'body': response
            }
        else:
            logger.info("File does not match pattern, no action taken.")
            return {
                'statusCode': 200,
                'body': 'File does not match pattern, no action taken.'
            }
    except client.exceptions.EntityNotFoundException as e:
        logger.error("EntityNotFoundException: %s", e)
        return {
            'statusCode': 404,
            'body': f"EntityNotFoundException: {e}"
        }
    except KeyError as e:
        logger.error("KeyError: %s", e)
        return {
            'statusCode': 400,
            'body': f"KeyError: {e}"
        }
    except Exception as e:
        logger.error("Exception: %s", e)
        return {
            'statusCode': 500,
            'body': f"Exception: {e}"
        }
