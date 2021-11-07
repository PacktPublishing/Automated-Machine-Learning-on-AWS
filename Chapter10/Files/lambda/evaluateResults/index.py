import os
import json
import logging
import boto3
import botocore
from botocore.exceptions import ClientError
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)
s3 = boto3.client("s3")
ssm = boto3.client("ssm")
sm = boto3.client("sagemaker")


def lambda_handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)

    if ("evaluationFile" in event):
        evaluation_file = event["evaluationFile"]
    else:
        raise KeyError("'S3 Key for Evaluation File' not found in Lambda event!")

    logger.info("Reading Evaluation Report")
    try:
        obj = s3.get_object(Bucket=os.environ["BUCKET"], Key=evaluation_file)["Body"].read()
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    
    current_report = json.loads(obj)
    logger.info(f"Current Evaluation Report: {current_report}")
    current_rmse = current_report["regression_metrics"]["rmse"]["value"]

    logger.info("Reading Previous Model's Evaluation Report")
    model_package = get_package(os.environ["PACKAGE_PARAMETER"])
    if model_package != "PLACEHOLDER":
        try:
            uri = sm.describe_model_package(
                ModelPackageName=model_package
            )["ModelMetrics"]["ModelQuality"]["Statistics"]["S3Uri"]
            bucket = urlparse(uri).netloc
            key = urlparse(uri).path.lstrip("/")
            previous_obj = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except ClientError as e:
            error_message = e.response["Error"]["Message"]
            logger.error(error_message)
            raise Exception(error_message)
        
        previous_report = json.loads(previous_obj)
        logger.info(f"Previous Evaluation Report: {previous_report}")
        previous_rmse = previous_report ["regression_metrics"]["rmse"]["value"]

        if current_rmse < previous_rmse:
            improved = "TRUE"
        else:
            improved = "FALSE"
    else:
        improved = "TRUE"
    logger.info(f"Model Improved: {improved}")

    return {
        'statusCode': 200,
        'rmse': current_rmse,
        'improved': improved
    }


def get_package(parameter_name):
    try:
        package = ssm.get_parameter(
            Name=parameter_name
        )['Parameter']['Value']

        return package

    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(error_message)
        raise Exception(error_message)
