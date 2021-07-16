import os
import json
import logging
import boto3
import botocore
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
cp = boto3.client('codepipeline')
sm = boto3.client('sagemaker')

# # Define the mapping for 'sklearn' container uri's
# image_map = {
#     # 'analyzer': {
#     #     'us-east-1': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'us-east-2': '777275614652.dkr.ecr.us-east-2.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'us-west-1': '890145073186.dkr.ecr.us-west-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'us-west-2': '159807026194.dkr.ecr.us-west-2.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'af-south-1': '875698925577.dkr.ecr.af-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'ap-east-1': '001633400207.dkr.ecr.ap-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'ap-northeast-1': '574779866223.dkr.ecr.ap-northeast-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'ap-northeast-2': '709848358524.dkr.ecr.ap-northeast-2.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'ap-south-1': '126357580389.dkr.ecr.ap-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'ap-southeast-1': '245545462676.dkr.ecr.ap-southeast-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'ap-southeast-2': '563025443158.dkr.ecr.ap-southeast-2.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'ca-central-1': '536280801234.dkr.ecr.ca-central-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'cn-north-1': '453000072557.dkr.ecr.cn-north-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'cn-northwest-1': '453252182341.dkr.ecr.cn-northwest-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'eu-central-1': '048819808253.dkr.ecr.eu-central-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'eu-north-1': '895015795356.dkr.ecr.eu-north-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'eu-south-1': '933208885752.dkr.ecr.eu-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'eu-west-1': '468650794304.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'eu-west-2': '749857270468.dkr.ecr.eu-west-2.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'eu-west-3': '680080141114.dkr.ecr.eu-west-3.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'me-south-1': '607024016150.dkr.ecr.me-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
#     #     'sa-east-1': '539772159869.dkr.ecr.sa-east-1.amazonaws.com/sagemaker-model-monitor-analyzer'
#     # },
#     # 'sklearn': {
#         'us-west-1': '746614075791.dkr.ecr.us-west-1.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'us-west-2': '246618743249.dkr.ecr.us-west-2.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'us-east-1': '683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'us-east-2': '257758044811.dkr.ecr.us-east-2.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'ap-northeast-1': '354813040037.dkr.ecr.ap-northeast-1.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'ap-northeast-2': '366743142698.dkr.ecr.ap-northeast-2.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'ap-southeast-1': '121021644041.dkr.ecr.ap-southeast-1.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'ap-southeast-2': '783357654285.dkr.ecr.ap-southeast-2.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'ap-south-1': '720646828776.dkr.ecr.ap-south-1.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'eu-west-1': '141502667606.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'eu-west-2': '764974769150.dkr.ecr.eu-west-2.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'eu-central-1': '492215442770.dkr.ecr.eu-central-1.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#         'ca-central-1': '341280168497.dkr.ecr.ca-central-1.amazonaws.com/sagemaker-scikit-learn:0.23-1-cpu-py3',
#     # }
# }


def lambda_handler(event, context):
    logger.debug('-- Environment Variables --')
    logger.debug(os.environ)
    logger.debug('-- Event --')
    logger.debug(event)
    logger.info('Creating SageMaker Experiment')

    # Ensure variables are passed from the invocation inputs
    if ("modelName" in event):
        model_name = event['modelName']
    else:
        raise KeyError("'Model Name' not found in Lambda event!")
    
    if ("pipelineName" in event):
        pipeline_name = event['pipelineName']
    else:
        raise KeyError("'Pipeline Name' not found in Lambda event!")
    
    if ("stageName" in event):
        stage_name = event['stageName']
    else:
        raise KeyError("'Pipeline Stage Name' not in Lambda event!")
    
    if ("actionName" in event):
        action_name = event['actionName']
    else:
        raise KeyError("'Pipeline Action Name' not in Lambda event!")
    
    if ("dataBucket" in event):
        data_bucket = event['dataBucket']
    else:
        raise KeyError("'Data Bucket Name' not found in Lambda event!")
    
    # Get the current CodePipeline Execution ID
    execution_id = get_executionId(pipeline_name, stage_name, action_name)

    # Create/Verify SageMaker Experiment  and Trial Names
    experiment_name, trial_name = create_experiment(model_name, execution_id)

    # Generate the 'input' payload for the rest of the State Machine
    payload = {
        'statusCode': 200,
        # 'modelName': model_name,
        'executionId': execution_id,
        'experimentName': experiment_name,
        'trialName': trial_name,
        'processingJobName': f'{model_name}-processing-{execution_id}',
        'processingCodeInput': f's3://{data_bucket}/scripts/preprocessing.py',
        'processingDataInput': f's3://{data_bucket}/input/raw/abalone.csv',
        'processingTrainingOutput': f's3://{data_bucket}/{execution_id}/input/training',
        'processingTestingOutput': f's3://{data_bucket}/{execution_id}/input/testing',
        'processingBaselineOutput': f's3://{data_bucket}/{execution_id}/input/baseline',
        # 'processingImage': image_map['sklearn'][os.environ['AWS_DEFAULT_REGION']],
        # 'processingImage': image_map[os.environ['AWS_DEFAULT_REGION']],
        'trainingJobName': f'{model_name}-training-{execution_id}',
        'trainingDataInput': f's3://{data_bucket}/{execution_id}/input/training',
        'trainingModelOutput': f's3://{data_bucket}/{execution_id}/',
        'evaluationJobName': f'{model_name}-evaluation-{execution_id}',
        'evaluationCodeInput': f's3://{data_bucket}/scripts/evaluation.py',
        'evaluationDataInput': f's3://{data_bucket}/{execution_id}/input/testing/testing.csv',
        'evaluationOutput': f's3://{data_bucket}/{execution_id}/input/evaluation',
        'evaluationOutputFile': f'{execution_id}/input/evaluation/evaluation.json',
        # 'baselineJobName': f'{model_name}-baseline-{execution_id}',
        'baselineDataInput': f's3://{data_bucket}/{execution_id}/input/baseline/baseline.csv',
        # 'baselineImage': image_map['analyzer'][os.environ['AWS_DEFAULT_REGION']],
    }

    # Return payload
    return payload


def get_executionId(pipeline_name, stage_name, action_name):
    logger.info(f'Getting the latest CodePipeline Execution ID for {pipeline_name}')

    # Get the current CodePipeline Execuiton ID for the 'MLWorkflow.Execute' stage action
    try:
        response = cp.get_pipeline_state(name=pipeline_name)
        for stageState in response['stageStates']:
            if stageState['stageName'] == stage_name:
                for actionState in stageState['actionStates']:
                    if actionState['actionName'] == action_name:
                        executionId = stageState['latestExecution']['pipelineExecutionId']
    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(error_message)
        raise Exception(error_message)
    
    # Return the CodePipeline Execution ID
    logger.info(f'Current Pipeline Execution ID: {executionId}')
    return executionId


def create_experiment(model_name, execution_id):
    experiment_name = f'{model_name.capitalize()}Experiments'
    trial_name = f'{model_name.capitalize()}-{execution_id}'
    logger.info('Getting list of SageMaker Experiments')
    try:
        response = sm.list_experiments(
            SortBy='Name',
            MaxResults=100
        )
        names = [experiments['ExperimentName'] for experiments in response['ExperimentSummaries']]
    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(error_message)
        raise Exception(error_message)
    
    # Create the Experiment if not already created
    logger.info(f'Checking if Experiment already exists')
    if experiment_name not in names:
        try:
            response = sm.create_experiment(
                ExperimentName=experiment_name,
                Description=f'Training Experiments for {model_name}',
            )
            logger.info(f'Created SageMaker Experiment: {experiment_name}')
        except ClientError as e:
            error_message = e.response['Error']['Message']
            logger.error(error_message)
            raise Exception(error_message)
    
    # Create a Trial to associate with the execution of the pipeline
    logger.info(f'Creating Associated SageMaker Trial')
    try:
        response = sm.create_trial(
            ExperimentName=experiment_name,
            TrialName=trial_name
        )
    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(error_message)
        raise Exception(error_message)
    
    # Return the generated experiment and trial
    return experiment_name, trial_name