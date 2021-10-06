import boto3
import os
import json
import time
# import uuid
import sys
import errno
import logging
# from time import strftime

client = boto3.client('stepfunctions')
# sfn_arn = os.environ['STATEMACHINE_ARN']
# event_id = strftime('%Y%m%d%H%M%S')
logger = logging.getLogger()
log_format = "%(levelname)s: [%(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=log_format, level=os.environ.get("LOGLEVEL", "INFO").upper())
input = {
    "input": {
        # "processing_job_name": "mlops-processing-{}".format(event_id),
        # "training_job_name": "mlops-training-{}".format(event_id),
        # "evaluation_job_name": "mlops-evaluation-{}".format(event_id),
        # "baseline_job_name": "mlops-baseline-{}".format(event_id)
        "model_name": os.environ['MODEL_NAME'],
        "pipeline_name": os.environ['PIPELINE_NAME'],
        "stage_name": os.environ['STAGE_NAME'],
        "action_name": os.environ['ACTION_NAME'],
        "data_bucket": os.environ['DATA_BUCKET']
  }
}


# Start the execution
logger.info(f'Invoking ML Workflow: {os.environ["STATEMACHINE_ARN"]}')
execution_arn = client.start_execution(
    stateMachineArn=os.environ['STATEMACHINE_ARN'],
    input=json.dumps(input)
)['executionArn']

status = client.describe_execution(executionArn=execution_arn)['status']
while status == "RUNNING":
    time.sleep(60)
    logger.info("ML Workflow Status: {}".format(status))
    status = client.describe_execution(executionArn=execution_arn)['status']
if status == "SUCCEEDED":
    logger.info("ML Workflow Exection: {}".format(status))
    sys.exit(os.EX_OK)
else:
    error_message = "ML Workflow execution: {}".format(status)
    logger.error(error_message)
    sys.exit(errno.EACCES)
