import boto3
import logging
import os
import json
import sys
from botocore.exceptions import ClientError

logger = logging.getLogger()
logging_format = "%(levelname)s: [%(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=logging_format, level=os.environ.get("LOGLEVEL", "INFO").upper())
codepipeline_client = boto3.client("codepipeline")
sagemaker_client = boto3.client("sagemaker")
pipeline_name = os.environ["PIPELINE_NAME"]
model_name = os.environ["MODEL_NAME"]
#role_arn = os.environ["ROLE_ARN"]


def get_execution_id(name=None, task=None):
    try:
        response = codepipeline_client.get_pipeline_state(name=name)
        for stage in response["stageStates"]:
            if stage["stageName"] == "Deploy":
                for action in stage["actionStates"]:
                    if action["actionName"] == task:
                        return stage["latestExecution"]["pipelineExecutionId"]
    except ClientError as e:
        error = e.response["Error"]["Message"]
        logger.error(error)
        raise Exception(error)


if __name__ == "__main__":
    task = "DeploymentBuild"
    execution_id = get_execution_id(name=pipeline_name, task=task)
    logger.info("Creating Stack Parameters")
    params = {
        "ExecutionId": execution_id,
        "BucketName": os.environ["BUCKET_NAME"],
        #"ExecutionRole": os.environ["ROLE_ARN"]
    }
    try:
        with open(os.path.join(os.environ["CODEBUILD_SRC_DIR"], "output/params.json"), "w") as f:
            json.dump(params, f)
        logger.info(json.dumps(params, indent=4)),
        sys.exit(0)
    except Exception as error:
        logger.error(error)
        sys.exit(255)