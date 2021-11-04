import os
import json
import logging
import boto3
import botocore
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
cp = boto3.client("codepipeline")
sm = boto3.client("sagemaker")


def lambda_handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)
    logger.info('Creating SageMaker Experiment')

    if ("modelName" in event):
        model_name = event["modelName"]
    else:
        raise KeyError("'Model Name' not found in Lambda event!")
    
    if ("pipelineName" in event):
        pipeline_name = event["pipelineName"]
    else:
        raise KeyError("'Pipeline Name' not found in Lambda event!")
    
    if ("stageName" in event):
        stage_name = event["stageName"]
    else:
        raise KeyError("'Pipeline Stage Name' not in Lambda event!")
    
    if ("actionName" in event):
        action_name = event["actionName"]
    else:
        raise KeyError("'Pipeline Action Name' not in Lambda event!")
    
    if ("dataBucket" in event):
        data_bucket = event["dataBucket"]
    else:
        raise KeyError("'Data Bucket Name' not found in Lambda event!")
    
    execution_id = get_executionId(pipeline_name, stage_name, action_name)
    experiment_name, trial_name = create_experiment(model_name, execution_id)

    payload = {
        "statusCode": 200,
        "executionId": execution_id,
        "experimentName": experiment_name,
        "trialName": trial_name,
        "processingJobName": f"{model_name}-processing-{execution_id}",
        "processingCodeInput": f"s3://{data_bucket}/scripts/preprocessing.py",
        "processingTrainingOutput": f"s3://{data_bucket}/{execution_id}/input/training",
        "processingTestingOutput": f"s3://{data_bucket}/{execution_id}/input/testing",
        "processingBaselineOutput": f"s3://{data_bucket}/{execution_id}/input/baseline",
        "trainingJobName": f"{model_name}-training-{execution_id}",
        "trainingDataInput": f"s3://{data_bucket}/{execution_id}/input/training",
        "trainingModelOutput": f"s3://{data_bucket}/{execution_id}/",
        "evaluationJobName": f"{model_name}-evaluation-{execution_id}",
        "evaluationCodeInput": f"s3://{data_bucket}/scripts/evaluation.py",
        "evaluationDataInput": f"s3://{data_bucket}/{execution_id}/input/testing/testing.csv",
        "evaluationOutput": f"s3://{data_bucket}/{execution_id}/input/evaluation",
        "evaluationOutputFile": f"{execution_id}/input/evaluation/evaluation.json",
        "baselineDataInput": f"s3://{data_bucket}/{execution_id}/input/baseline/baseline.csv",
    }

    return payload


def get_executionId(pipeline_name, stage_name, action_name):
    logger.info(f"Getting the latest CodePipeline Execution ID for {pipeline_name}")
    try:
        response = cp.get_pipeline_state(name=pipeline_name)
        for stageState in response["stageStates"]:
            if stageState["stageName"] == stage_name:
                for actionState in stageState["actionStates"]:
                    if actionState["actionName"] == action_name:
                        executionId = stageState["latestExecution"]["pipelineExecutionId"]
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)

    logger.info(f"Current Pipeline Execution ID: {executionId}")
    return executionId


def create_experiment(model_name, execution_id):
    experiment_name = f"{model_name.capitalize()}Experiments"
    trial_name = f"{model_name.capitalize()}-{execution_id}"
    logger.info("Getting list of SageMaker Experiments")
    try:
        response = sm.list_experiments(
            SortBy="Name",
            MaxResults=100
        )
        names = [experiments["ExperimentName"] for experiments in response["ExperimentSummaries"]]
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    
    logger.info(f"Checking if Experiment already exists")
    if experiment_name not in names:
        try:
            response = sm.create_experiment(
                ExperimentName=experiment_name,
                Description=f"Training Experiments for {model_name}",
            )
            logger.info(f"Created SageMaker Experiment: {experiment_name}")
        except ClientError as e:
            error_message = e.response["Error"]["Message"]
            logger.error(error_message)
            raise Exception(error_message)

    logger.info(f'Creating Associated SageMaker Trial')
    try:
        response = sm.create_trial(
            ExperimentName=experiment_name,
            TrialName=trial_name
        )
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    
    return experiment_name, trial_name