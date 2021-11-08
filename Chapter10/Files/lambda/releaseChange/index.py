import os
import logging
import boto3
import botocore
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
cp = boto3.client("codepipeline")


def lambda_handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)
    pipeline_name = os.environ["PIPELINE_NAME"]
    logger.info(f"Starting Coninuous Training release change for {pipeline_name}")
    try:
        response = cp.start_pipeline_execution(
            name=pipeline_name
        )
        logger.info(f'Release Change ExecutionId: {response["pipelineExecutionId"]}')
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    return {
        "statusCode": 200,
        "ExecutionId": response["pipelineExecutionId"]
    }