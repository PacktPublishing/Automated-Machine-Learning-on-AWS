import os
import json
import logging
import boto3
import botocore
from botocore.exceptions import ClientError
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client('s3')
ssm = boto3.client('ssm')
sm = boto3.client('sagemaker')

def handler_handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)

    # Ensure variables are passed from the step payload
    if ("evaluationFile" in event):
        evaluation_file = event['evaluationFile']
    else:
        raise KeyError("'S3 Key for Evaluation File' not found in Lambda event!")

    # Read model evaluation report from S3
    logger.info('Reading Evaluation Report')
    try:
        # obj = s3.get_object(Bucket=os.environ['BUCKET'],Key=os.environ['KEY'])['Body'].read()
        obj = s3.get_object(Bucket=os.environ['BUCKET'], Key=evaluation_file)['Body'].read()
        #logger.info('Done!')
    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(error_message)
        raise Exception(error_message)
    
    # Load report dictionary
    current_report = json.loads(obj)
    logger.info(f'Current Evaluation Report: {current_report}')
    current_rmse = current_report['regression_metrics']['rmse']['value']

    # Read the model evaluation for the current production model (if it exists)
    logger.info("Reading Previous Model's Evaluation Report")
    model_package = get_package(os.environ['PACKAGE_PARAMETER'])
    if model_package != 'PLACEHOLDER':
        try:
            # Get the location of the existing model's evaluation report
            uri = sm.describe_model_package(
                ModelPackageName=model_package
            )['ModelMetrics']['ModelQuality']['Statistics']['S3Uri']
            bucket = urlparse(uri).netloc
            key = urlparse(uri).path.lstrip('/')
            previous_obj = s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        except ClientError as e:
            error_message = e.response['Error']['Message']
            logger.error(error_message)
            raise Exception(error_message)
        
        # Get the rmse for the existing model
        previous_report = json.loads(previous_obj)
        logger.info(f'Previous Evaluation Report: {previous_report}')
        previous_rmse = previous_report ['regression_metrics']['rmse']['value']

        # Determine if the current model improves on the existng mode
        if current_rmse < previous_rmse:
            improved = 'TRUE'
        else:
            improved = 'FALSE'
    else:
        # There is no existing produciton model, therefore the current model is an improvement
        improved = 'TRUE'
    logger.info(f'Model Improved: {improved}')

    # Return the payload values
    return {
        'statusCode': 200,
        'rmse': current_rmse,
        'improved': improved
    }


def get_package(parameter_name):
    try:
        # Get the latest model package ARN from SSM parameter store
        package = ssm.get_parameter(
            Name=parameter_name
        )['Parameter']['Value']

        # Return the ARN for the latest model package
        return package

    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(error_message)
        raise Exception(error_message)
