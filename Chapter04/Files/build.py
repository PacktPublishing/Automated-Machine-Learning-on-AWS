import boto3
import logging
import os
import sys
import time
from botocore.exceptions import ClientError

logger = logging.getLogger()
logging_format = "%(levelname)s: [%(filename)s:%(lineno)s] %(message)s"
logging.basicConfig(format=logging_format, level=os.environ.get("LOGLEVEL", "INFO").upper())
codepipeline_client = boto3.client("codepipeline")
sagemaker_client = boto3.client("sagemaker")
image_uri = os.environ["IMAGE_URI"]
bucket_name = os.environ["BUCKET_NAME"]
role_arn = os.environ["ROLE_ARN"]
pipeline_name = os.environ["PIPELINE_NAME"]
model_name = os.environ["MODEL_NAME"]


def get_execution_id(name=None, task=None):
    try:
        response = codepipeline_client.get_pipeline_state(name=name)
        for stage in response["stageStates"]:
            if stage["stageName"] == "Build":
                for action in stage["actionStates"]:
                    if action["actionName"] == task.capitalize():
                        return stage["latestExecution"]["pipelineExecutionId"]
    except ClientError as e:
        error = e.response["Error"]["Message"]
        logger.error(error)
        raise Exception(error)


def handle_status(task=None, job_name=None):
    if task == "preprocess" or task == "evaluate":
        status = sagemaker_client.describe_processing_job(ProcessingJobName=job_name)["ProcessingJobStatus"]
        while status == "InProgress":
            time.sleep(60)
            logger.info(f"Task: {task},  Status: {status}")
            status = sagemaker_client.describe_processing_job(ProcessingJobName=job_name)["ProcessingJobStatus"]
        return status
    elif task == "train":
        status = sagemaker_client.describe_training_job(TrainingJobName=job_name)["TrainingJobStatus"]
        while status == "InProgress":
            time.sleep(60)
            logger.info(f"Task: {task}, Status: {status}")
            status = sagemaker_client.describe_training_job(TrainingJobName=job_name)["TrainingJobStatus"]
        return status


def get_model_artifact(name=None):
    try:
        response = sagemaker_client.describe_training_job(TrainingJobName=name)
        return response["ModelArtifacts"]["S3ModelArtifacts"]
    except ClientError as e:
        error = e.response["Error"]["Message"]
        logger.error(error)
        raise Exception(error)


def handle_data(model_name=None, execution_id=None):
    try:
        response = sagemaker_client.create_processing_job(
            ProcessingJobName=f"{model_name}-ProcessingJob-{execution_id}",
            ProcessingResources={
                'ClusterConfig': {
                    'InstanceCount': 1,
                    'InstanceType': 'ml.m5.xlarge',
                    'VolumeSizeInGB': 30
                }
            },
            StoppingCondition={
                'MaxRuntimeInSeconds': 3600
            },
            AppSpecification={
                'ImageUri': f"{image_uri}:latest",
                'ContainerEntrypoint': ["python", "app.py", "preprocess"]
            },
            ProcessingInputs=[
                {
                    'InputName': 'data',
                    'S3Input': {
                        'S3Uri': f"s3://{bucket_name}/data/{model_name}.data",
                        'LocalPath': '/opt/ml/processing/input/data',
                        'S3DataType': 'S3Prefix',
                        'S3InputMode': 'File',
                        'S3DataDistributionType': 'FullyReplicated',
                        'S3CompressionType': 'None'
                    }
                }
            ],
            ProcessingOutputConfig={
                'Outputs': [
                    {
                        'OutputName': 'training',
                        'S3Output': {
                            'S3Uri': f"s3://{bucket_name}/{execution_id}/input/training",
                            'LocalPath': '/opt/ml/processing/output/training',
                            'S3UploadMode': 'EndOfJob'
                        }
                    },
                    {
                        'OutputName': 'testing',
                        'S3Output': {
                            'S3Uri': f"s3://{bucket_name}/{execution_id}/input/testing",
                            'LocalPath': '/opt/ml/processing/output/testing',
                            'S3UploadMode': 'EndOfJob'
                        }
                    }
                ]
            },
            RoleArn=role_arn
        )
        return f"{model_name}-ProcessingJob-{execution_id}"
    except ClientError as e:
        error = e.response["Error"]["Message"]
        logger.error(error)
        raise Exception(error)


def handle_training(model_name=None, execution_id=None):
    try:
        response = sagemaker_client.create_training_job(
            TrainingJobName=f"{model_name}-TrainingJob-{execution_id}",
            AlgorithmSpecification={
                'TrainingImage': f"{image_uri}:latest",
                'TrainingInputMode': 'File',
                'EnableSageMakerMetricsTimeSeries': True,
                'MetricDefinitions': [
                    {
                        'Name': 'loss',
                        'Regex': 'loss: ([0-9\\.]+)'
                    },
                    {
                        'Name': 'mae',
                        'Regex': 'mae: ([0-9\\.]+)'
                    },
                    {
                        'Name': 'validation_loss',
                        'Regex': 'val_loss: ([0-9\\.]+)'
                    },
                    {
                        'Name': 'validation_mae',
                        'Regex': 'val_mae: ([0-9\\.]+)'
                    }
                ]
            },
            HyperParameters={
                'epochs': '200',
                'batch_size': '8'
            },
            InputDataConfig=[
                {
                    'ChannelName': 'training',
                    'ContentType': 'text/csv',
                    'DataSource': {
                        'S3DataSource': {
                            'S3Uri': f"s3://{bucket_name}/{execution_id}/input/training",
                            'S3DataType': 'S3Prefix',
                            'S3DataDistributionType': 'FullyReplicated'
                        }
                    }
                }
            ],
            OutputDataConfig={
                'S3OutputPath': f"s3://{bucket_name}/{execution_id}"
            },
            ResourceConfig={
                'InstanceType': 'ml.m5.xlarge',
                'InstanceCount': 1,
                'VolumeSizeInGB': 30
            },
            RoleArn=role_arn,
            StoppingCondition={
                'MaxRuntimeInSeconds': 3600
            }
        )
        return f"{model_name}-TrainingJob-{execution_id}"
    except ClientError as e:
        error = e.response["Error"]["Message"]
        logger.error(error)
        raise Exception(error)


def handle_evaluation(model_name=None, execution_id=None):
    try:
        response = sagemaker_client.create_processing_job(
            ProcessingJobName=f"{model_name}-EvaluationJob-{execution_id}",
            ProcessingResources={
                'ClusterConfig': {
                    'InstanceCount': 1,
                    'InstanceType': 'ml.m5.xlarge',
                    'VolumeSizeInGB': 30
                }
            },
            StoppingCondition={
                'MaxRuntimeInSeconds': 3600
            },
            AppSpecification={
                'ImageUri': f"{image_uri}:latest",
                'ContainerEntrypoint': ["python", "app.py", "evaluate"]
            },
            ProcessingInputs=[
                {
                    'InputName': 'data',
                    'S3Input': {
                        'S3Uri': f"s3://{bucket_name}/{execution_id}/input/testing",
                        'LocalPath': '/opt/ml/processing/input/data',
                        'S3DataType': 'S3Prefix',
                        'S3InputMode': 'File',
                        'S3DataDistributionType': 'FullyReplicated',
                        'S3CompressionType': 'None'
                    }
                },
                {
                    'InputName': 'model',
                    'S3Input': {
                        'S3Uri': get_model_artifact(name=f"{model_name}-TrainingJob-{execution_id}"),
                        'LocalPath': '/opt/ml/processing/input/model',
                        'S3DataType': 'S3Prefix',
                        'S3InputMode': 'File',
                        'S3DataDistributionType': 'FullyReplicated',
                        'S3CompressionType': 'None'
                    }
                }
            ],
            ProcessingOutputConfig={
                'Outputs': [
                    {
                        'OutputName': 'evaluation',
                        'S3Output': {
                            'S3Uri': f"s3://{bucket_name}/{execution_id}/evaluation",
                            'LocalPath': '/opt/ml/processing/output/evaluation',
                            'S3UploadMode': 'EndOfJob'
                        }
                    }
                ]
            },
            RoleArn=role_arn
        )
        return f"{model_name}-EvaluationJob-{execution_id}"
    except ClientError as e:
        error = e.response["Error"]["Message"]
        logger.error(error)
        raise Exception(error)


if __name__ == "__main__":
    task = sys.argv[1]
    execution_id = get_execution_id(name=pipeline_name, task=task)
    logger.info(f"Executing {task.upper()} task")
    if task == "preprocess":
        job_name = handle_data(model_name=model_name, execution_id=execution_id)
        status = handle_status(task=task, job_name=job_name)
    elif task == "train":
        job_name = handle_training(model_name=model_name, execution_id=execution_id)
        status = handle_status(task=task, job_name=job_name)
    elif task == "evaluate":
        job_name = handle_evaluation(model_name=model_name, execution_id=execution_id)
        status = handle_status(task=task, job_name=job_name)
    else:
        error = "Invalid argument: Specify 'preprocess', 'train' or 'evaluate'"
        logger.error(error)
        sys.exit(255)
    if status == "Completed":
        logger.info(f"Task: {task}, Final Status: {status}")
        sys.exit(0)
    else:
        error = f"Task: {task}, Failed! See CloudWatch Logs for further information"
        logger.error(error)
        sys.exit(255)