import boto3
import os
import json
import time
import sys
import logging

sfn = boto3.client("stepfunctions")
logger = logging.getLogger()
log_format = "%(levelname)s: [%(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=log_format, level=os.environ.get("LOGLEVEL", "INFO").upper())
logger.info(f'Invoking ML Workflow: {os.environ["STATEMACHINE_ARN"]}')
execution_arn = sfn.start_execution(
    stateMachineArn=os.environ['STATEMACHINE_ARN'],
    input=json.dumps(
        {
            "input": {
                "model_name": os.environ["MODEL_NAME"],
                "pipeline_name": os.environ["PIPELINE_NAME"],
                "stage_name": os.environ["STAGE_NAME"],
                "action_name": os.environ["ACTION_NAME"],
                "data_bucket": os.environ["DATA_BUCKET"]
            }
        }
    )
)["executionArn"]
status = sfn.describe_execution(executionArn=execution_arn)["status"]
while status == "RUNNING":
    time.sleep(60)
    logger.info("ML Workflow Status: {}".format(status))
    status = sfn.describe_execution(executionArn=execution_arn)["status"]
if status == "SUCCEEDED":
    logger.info("ML Workflow Exection: {}".format(status))
    sys.exit(0)
else:
    error_message = "ML Workflow execution: {}".format(status)
    logger.error(error_message)
    sys.exit(255)
