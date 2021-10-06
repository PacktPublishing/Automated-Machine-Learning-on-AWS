import io
import json
import os
import logging
import boto3
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from datetime import datetime


s3 = boto3.resource('s3')
sm = boto3.client('sagemaker')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Define the mapping for 'baseline-analyzer' container uri's
image_map = {
    'us-east-1': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'us-east-2': '777275614652.dkr.ecr.us-east-2.amazonaws.com/sagemaker-model-monitor-analyzer',
    'us-west-1': '890145073186.dkr.ecr.us-west-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'us-west-2': '159807026194.dkr.ecr.us-west-2.amazonaws.com/sagemaker-model-monitor-analyzer',
    'af-south-1': '875698925577.dkr.ecr.af-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'ap-east-1': '001633400207.dkr.ecr.ap-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'ap-northeast-1': '574779866223.dkr.ecr.ap-northeast-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'ap-northeast-2': '709848358524.dkr.ecr.ap-northeast-2.amazonaws.com/sagemaker-model-monitor-analyzer',
    'ap-south-1': '126357580389.dkr.ecr.ap-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'ap-southeast-1': '245545462676.dkr.ecr.ap-southeast-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'ap-southeast-2': '563025443158.dkr.ecr.ap-southeast-2.amazonaws.com/sagemaker-model-monitor-analyzer',
    'ca-central-1': '536280801234.dkr.ecr.ca-central-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'cn-north-1': '453000072557.dkr.ecr.cn-north-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'cn-northwest-1': '453252182341.dkr.ecr.cn-northwest-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'eu-central-1': '048819808253.dkr.ecr.eu-central-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'eu-north-1': '895015795356.dkr.ecr.eu-north-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'eu-south-1': '933208885752.dkr.ecr.eu-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'eu-west-1': '468650794304.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'eu-west-2': '749857270468.dkr.ecr.eu-west-2.amazonaws.com/sagemaker-model-monitor-analyzer',
    'eu-west-3': '680080141114.dkr.ecr.eu-west-3.amazonaws.com/sagemaker-model-monitor-analyzer',
    'me-south-1': '607024016150.dkr.ecr.me-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
    'sa-east-1': '539772159869.dkr.ecr.sa-east-1.amazonaws.com/sagemaker-model-monitor-analyzer'
}


def lambda_handler(event, context):
    logger.info('Received Event: {}'.format(json.dumps(event, indent=2)))
    props = event['ResourceProperties']

    # Baseline source
    source_bucket = urlparse(props['BaselineSourceUri']).netloc
    source_key = urlparse(props['BaselineSourceUri']).path.lstrip('/')

    # Destination bucket parameters
    logs_bucket = props['LogsBucketName']

    if event['RequestType'] != 'Delete':
        # Download the baseline dataset and copy it to the 'logs' bucket
        logger.info(f'Copying data from {source_bucket} to {logs_bucket}.')
        try:
            s3.meta.client.copy({'Bucket': source_bucket, 'Key': source_key}, logs_bucket, 'baselining/data/baseline.csv')
        except ClientError as e:
            error_message = e.response['Error']['Message']
            logger.error(error_message)
            raise Exception(error_message)
        
        # Create Baseline Suggestion request
        request = {
            'ProcessingJobName': f'abalone-baseline-{datetime.utcnow():%Y-%m-%d-%H%M}', 
            'Environment': {
                'analysis_type': 'MODEL_QUALITY',
                'dataset_format': '{"csv": {"header": true, "output_columns_position": "START"}}',
                'dataset_source': '/opt/ml/processing/input/baseline_dataset_input',
                'ground_truth_attribute': 'label',
                'inference_attribute': 'prediction',
                'output_path': '/opt/ml/processing/output',
                'problem_type': 'Regression',
                'publish_cloudwatch_metrics': 'Disabled'
            },
            'AppSpecification': {
                'ImageUri': image_map[os.environ['AWS_DEFAULT_REGION']]
                # 'ContainerEntrypoint': None,
                # 'ContainerArguments': None
            },
            'ProcessingInputs': [
                {
                    'InputName': 'baseline_dataset_input',
                    'AppManaged': False,
                    'S3Input': {
                        'LocalPath': '/opt/ml/processing/input/baseline_dataset_input',
                        'S3Uri': f's3://{logs_bucket}/baselining/data/baseline.csv',
                        'S3DataDistributionType': 'FullyReplicated',
                        'S3DataType': 'S3Prefix',
                        'S3InputMode': 'File',
                        'S3CompressionType': 'None'
                        # 'S3DownloadMode': 'StartOfJob'
                    }
                    # 'DatasetDefinition': None
                }
            ],
            'ProcessingOutputConfig': {
                'Outputs': [
                    {
                        'OutputName': 'monitoring_output',
                        'AppManaged': False,
                        'S3Output': {
                            'LocalPath': '/opt/ml/processing/output',
                            'S3Uri': f's3://{logs_bucket}/baselining/results',
                            'S3UploadMode': 'EndOfJob'
                        }
                        # 'FeatureStoreOutput': None
                    }
                ]
                # 'KmsKeyId': None
            },
            'ProcessingResources': {
                'ClusterConfig': {
                    'InstanceCount': 1,
                    'InstanceType': 'ml.m5.xlarge',
                    'VolumeSizeInGB': 20
                    # 'VolumeKmsKeyId': None
                }
            },
            'RoleArn': props['RoleArn'],
            'StoppingCondition': {
                'MaxRuntimeInSeconds': 1800
            }
        }

        # Create the basline job
        logger.info(f'Creating Basline Suggestion Job: {request["ProcessingJobName"]}')
        try:
            response = sm.create_processing_job(**request)
            return {
                'PhysicalResourceId': response['ProcessingJobArn'],
                'Data': {
                    'ProcessingJobName': request['ProcessingJobName'],
                    'BaselineResultsUri': f's3://{logs_bucket}/baselining/results'
                }
            }
        except ClientError as e:
            error_message = e.response['Error']['Message']
            logger.error(error_message)
            raise Exception(error_message)
