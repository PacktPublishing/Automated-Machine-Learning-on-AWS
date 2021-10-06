# The MLOps Pipeline MKV (Python)

## Initial Setup

This section details the steps to construct the pattern from scratch using version __1.95.1__ of AWS CDK.

1. Initialize development environment.
```
    export CDK_VERSION=1.95.1 && \
    python3 -m venv .venv && \
    source .venv/bin/activate && \
    python -m pip install -r requirements.txt
```
2. Bootstrap the Pipeline Repository.
```
    export CDK_NEW_BOOTSTRAP=1 && \
    export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query "Account" --output text) && \
    export AWS_DEFAULT_REGION=$(aws configure get default.region) && \
    npx cdk bootstrap \
    --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess \
    aws://$AWS_ACCOUNT_ID/$AWS_DEFAULT_REGION
```
2. Define the CodeCommit Repository and "empty" pipeline in `the_mlops_pipeline_stack.py`.
3. Deploy the "empty" pipeline.
```
    cdk deploy
```
>__NOTE:__ Even through the pipeline is created, execution will fail as there is no source code in the newly created repository.
4. Initialize the source code origin.
```
    git init && \
    git remote add origin https://git-codecommit.${AWS_DEFAULT_REGION}.amazonaws.com/v1/repos/mlops && \
    git add -A && \
    git commit -m "Initial commit" && \
    git push --set-upstream origin master
```
>__NOTE:__ This should trigger the Pipeline to self Build and Self Mutate as an "empty" pipeline.

---

# Notes

## Baseline for Model Quality

>__NOTE:__ The baseline component is no loger necessary for Version 0.1 as it will not be includedin Part One of the Blog. Keeping it here for posterity.

- In the previous versions a Data Quality Baseline was used. For this version, Model Quality monitoring is used and therefore this requires a different type of baseline data. The Data Quality Baseline uses the training dataset (with headers) while the Model Quality Baseline uses a dataset of predictions and ground truth labels. In essence, this is basically the output of the model evaluation step of the workflow. So for this version, the baseline data is captured during model evaluation and not as part of the actual workflow. 
- Because the Model Quality Schedule requires the inference Endpoint in place, as well as the baseline suggestion, the baseline is captured in the "PROD" stack and *NOT* the "ML Workflow" stack.
- Sample Request Syntax from SageMaker `suggest_baseline()` call:

```json
    {
        'ProcessingJobArn': 'arn:aws:sagemaker:us-east-2:500842391574:processing-job/abalone-baseline-job-2021-04-29-1638',
        'ProcessingJobName': 'abalone-baseline-job-2021-04-29-1638', 
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
            'ImageUri': '777275614652.dkr.ecr.us-east-2.amazonaws.com/sagemaker-model-monitor-analyzer',
            'ContainerEntrypoint': None,
            'ContainerArguments': None
        },
        'ProcessingInputs': [
            {
                'InputName': 'baseline_dataset_input',
                'AppManaged': False,
                'S3Input': {
                    'LocalPath': '/opt/ml/processing/input/baseline_dataset_input',
                    'S3Uri': 's3://proddeploymentstage-prodappl-logss3bucket004b0f70-10v6vdtnnhncu/baselining/data/baseline.csv',
                    'S3DataDistributionType': 'FullyReplicated',
                    'S3DataType': 'S3Prefix',
                    'S3InputMode': 'File',
                    'S3CompressionType': 'None',
                    'S3DownloadMode': 'StartOfJob'
                },
                'DatasetDefinition': None
            }
        ],
        'ProcessingOutputConfig': {
            'Outputs': [
                {
                    'OutputName': 'monitoring_output',
                    'AppManaged': False,
                    'S3Output': {
                        'LocalPath': '/opt/ml/processing/output',
                        'S3Uri': 's3://proddeploymentstage-prodappl-logss3bucket004b0f70-10v6vdtnnhncu/baselining/results',
                        'S3UploadMode': 'EndOfJob'
                    },
                    'FeatureStoreOutput': None
                }
            ],
            'KmsKeyId': None
        },
        'ProcessingResources': {
            'ClusterConfig': {
                'InstanceCount': 1,
                'InstanceType': 'ml.m5.xlarge',
                'VolumeSizeInGB': 20,
                'VolumeKmsKeyId': None
            }
        },
        'RoleArn': 'arn:aws:iam::500842391574:role/SageMaker',
        'StoppingCondition': {
            'MaxRuntimeInSeconds': 1800
        }
    }
```

- Origionally posted as an issue, the Baseline Suggestion function was not executing when the production CloudFormation stack updates. However, it seems that this was due to the fact that the "newer" model didn't improve over the previous version. Thus the model registry was not updated and therfore the "basline" (evaluation data) was not updated, so in essence the `BaselineDataUri` SSM parameter was not being updated. To verify this, I forced the `ModelPackageName` SSM parameter back to `PLACEHOLDER` so that the ML Workflow would be "tricked" into thinking the model had "improved". This created a new version in the model registry and thus the  `BaselineDataUri` was updated. Upon updating the productin deployment stack, CloudFormation saw the paramater update and triggered the execution of the baseline suggestion.

## Model Quality Monitoring Schedule

- Sample of what the 'merge' request looks like (from the CLoudWatch Logs):

```json
    {
        'ProcessingJobArn': 'arn:aws:sagemaker:us-east-2:500842391574:processing-job/groundtruth-merge-202105062300-de32d8acceeef6a1626b8fa6',
        'ProcessingJobName': 'groundtruth-merge-202105062300-de32d8acceeef6a1626b8fa6',
        'Environment': {
            'dataset_source': '/opt/ml/processing/input_data',
            'ground_truth_source': '/opt/ml/processing/groundtruth',
            'output_path': '/opt/ml/processing/output'
        },
        'AppSpecification': {
            'ImageUri': '777275614652.dkr.ecr.us-east-2.amazonaws.com/sagemaker-model-monitor-groundtruth-merger',
            'ContainerEntrypoint': None, 
            'ContainerArguments': None
        }, 
        'ProcessingInputs': [
            {
                'InputName': 'groundtruth_input_1', 
                'AppManaged': False, 
                'S3Input': {
                    'LocalPath': '/opt/ml/processing/groundtruth/2021/05/06/22', 
                    'S3Uri': 's3://proddeploymentstage-prodappl-logss3bucket004b0f70-al2qohgjyeh8/ground-truth-data/2021-05-06-22-33-42/2021/05/06/22', 
                    'S3DataDistributionType': 'FullyReplicated', 
                    'S3DataType': 'S3Prefix', 
                    'S3InputMode': 'File', 
                    'S3CompressionType': 'None', 
                    'S3DownloadMode': 'StartOfJob'
                },
                'DatasetDefinition': None
            },
            {
                'InputName': 'endpoint_input_1', 
                'AppManaged': False, 
                'S3Input': {
                    'LocalPath': '/opt/ml/processing/input_data/abalone-prod-endpoint/AllTraffic/2021/05/06/22', 
                    'S3Uri': 's3://proddeploymentstage-prodappl-logss3bucket004b0f70-al2qohgjyeh8/endpoint-data-capture/abalone-prod-endpoint/AllTraffic/2021/05/06/22', 
                    'S3DataDistributionType': 'FullyReplicated', 
                    'S3DataType': 'S3Prefix', 
                    'S3InputMode': 'File', 
                    'S3CompressionType': 'None', 
                    'S3DownloadMode': 'StartOfJob'
                },
                'DatasetDefinition': None
            }
        ],
        'ProcessingOutputConfig': {
            'Outputs': [
                {
                    'OutputName': 'result', 
                    'AppManaged': False,
                    'S3Output': {
                        'LocalPath': '/opt/ml/processing/output', 
                        'S3Uri': 's3://proddeploymentstage-prodappl-logss3bucket004b0f70-al2qohgjyeh8/baselining/results/merge', 
                        'S3UploadMode': 'EndOfJob'
                    },
                    'FeatureStoreOutput': None
                }
            ],
            'KmsKeyId': None
        },
        'ProcessingResources': {
            'ClusterConfig': {
                'InstanceCount': 1, 
                'InstanceType': 'ml.m5.xlarge', 
                'VolumeSizeInGB': 20, 
                'VolumeKmsKeyId': None
            }
        },
        'RoleArn': 'arn:aws:iam::500842391574:role/SageMaker', 
        'StoppingCondition': {
            'MaxRuntimeInSeconds': 1800
        }
    }
```

- Sample of what the Model Quality Processing REquest looks like (from CloudWatch Logs):

```json
    {
        'ProcessingJobArn': 'arn:aws:sagemaker:us-east-2:500842391574:processing-job/model-quality-monitoring-202105062300-de32d8acceeef6a1626b8fa6', 
        'ProcessingJobName': 'model-quality-monitoring-202105062300-de32d8acceeef6a1626b8fa6', 
        'Environment': {
            'analysis_type': 'MODEL_QUALITY', 
            'baseline_constraints': '/opt/ml/processing/baseline/constraints/constraints.json', 
            'dataset_format': '{"sagemakerMergeJson":{"captureIndexNames":["endpointOutput"]}}', 
            'dataset_source': '/opt/ml/processing/input_data', 
            'end_time': '2021-05-06T23:00:00Z', 
            'inference_attribute': '0', 
            'metric_time': '2021-05-06T22:00:00Z', 
            'output_path': '/opt/ml/processing/output', 
            'problem_type': 'Regression', 
            'publish_cloudwatch_metrics': 'Enabled', 
            'sagemaker_endpoint_name': 'abalone-prod-endpoint', 
            'sagemaker_monitoring_schedule_name': 'abalone-monitoring-schedule-2021-05-06-2240', 
            'start_time': '2021-05-06T22:00:00Z'
        },
        'AppSpecification': {
            'ImageUri': '777275614652.dkr.ecr.us-east-2.amazonaws.com/sagemaker-model-monitor-analyzer', 
            'ContainerEntrypoint': None, 
            'ContainerArguments': None
        },
        'ProcessingInputs': [
            {
                'InputName': 'constraints', 
                'AppManaged': False, 
                'S3Input': {
                    'LocalPath': '/opt/ml/processing/baseline/constraints', 
                    'S3Uri': 's3://proddeploymentstage-prodappl-logss3bucket004b0f70-al2qohgjyeh8/baselining/results/constraints.json', 
                    'S3DataDistributionType': 'FullyReplicated', 
                    'S3DataType': 'S3Prefix', 
                    'S3InputMode': 'File', 
                    'S3CompressionType': None, 
                    'S3DownloadMode': 'StartOfJob'
                },
                'DatasetDefinition': None
            },
            {
                'InputName': 'endpoint_input_1', 
                'AppManaged': False, 
                'S3Input': {
                    'LocalPath': '/opt/ml/processing/input_data/abalone-prod-endpoint/AllTraffic/2021/05/06/22', 
                    'S3Uri': 's3://proddeploymentstage-prodappl-logss3bucket004b0f70-al2qohgjyeh8/baselining/results/merge/abalone-prod-endpoint/AllTraffic/2021/05/06/22', 
                    'S3DataDistributionType': 'FullyReplicated', 
                    'S3DataType': 'S3Prefix', 
                    'S3InputMode': 'File', 
                    'S3CompressionType': 'None', 
                    'S3DownloadMode': 'StartOfJob'
                },
                'DatasetDefinition': None
            }
        ],
        'ProcessingOutputConfig': {
            'Outputs': [
                {
                    'OutputName': 'result', 
                    'AppManaged': False, 
                    'S3Output': {
                        'LocalPath': '/opt/ml/processing/output', 
                        'S3Uri': 's3://proddeploymentstage-prodappl-logss3bucket004b0f70-al2qohgjyeh8/baselining/results/abalone-prod-endpoint/abalone-monitoring-schedule-2021-05-06-2240/2021/05/06/23', 
                        'S3UploadMode': 'Continuous'
                    },
                    'FeatureStoreOutput': None
                }
            ],
            'KmsKeyId': None
        },
        'ProcessingResources': {
            'ClusterConfig': {
                'InstanceCount': 1, 
                'InstanceType': 'ml.m5.xlarge', 
                'VolumeSizeInGB': 20, 
                'VolumeKmsKeyId': None
            }
        },
        'RoleArn': 'arn:aws:iam::500842391574:role/SageMaker', 
        'StoppingCondition': {
            'MaxRuntimeInSeconds': 1800
        }
    }
```



---

# Issues

- Although not necessary for Part One of the blog post, there is an issue with the current way requests are being handled, which will impact the other part of the blog. Right now, the WEbsite form and the  `FormHAndler` processes an inference request example that is based on the testing data. This data is already preprocessed. However for later part of the blog, when checking the model quality, the syntehtic data will be based on the raw data, wich hasn't been preprocessed. So basically the data capture of inferences on the current infreence request will already be preprocessed. This is going to cause an issue for Part Two and Part Three of the blog. So the `FormHandler` and the website form will have to submit and process "origional" data so that the ground truth from theoriginal dataset can be correlated for model quality.

---

# Conclusion
