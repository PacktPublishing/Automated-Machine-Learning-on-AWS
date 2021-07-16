import os
import aws_cdk.core as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3_deployment
import aws_cdk.aws_ecr as ecr
import aws_cdk.aws_iam as iam
import aws_cdk.aws_ecr_assets as ecr_assets
import aws_cdk.aws_lambda as lambda_
import aws_cdk.custom_resources as cr
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_ssm as ssm


class MLWorkflowStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, id: str, *, group_name: str, threshold: float, **kwargs):
        super().__init__(scope, id, **kwargs)
        
        # # Define the mapping for 'sklearn' and 'baseline-analyzer' container uri's
        # image_map = {
        #     'analyzer': {
        #         'us-east-1': '156813124566.dkr.ecr.us-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'us-east-2': '777275614652.dkr.ecr.us-east-2.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'us-west-1': '890145073186.dkr.ecr.us-west-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'us-west-2': '159807026194.dkr.ecr.us-west-2.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'af-south-1': '875698925577.dkr.ecr.af-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'ap-east-1': '001633400207.dkr.ecr.ap-east-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'ap-northeast-1': '574779866223.dkr.ecr.ap-northeast-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'ap-northeast-2': '709848358524.dkr.ecr.ap-northeast-2.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'ap-south-1': '126357580389.dkr.ecr.ap-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'ap-southeast-1': '245545462676.dkr.ecr.ap-southeast-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'ap-southeast-2': '563025443158.dkr.ecr.ap-southeast-2.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'ca-central-1': '536280801234.dkr.ecr.ca-central-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'cn-north-1': '453000072557.dkr.ecr.cn-north-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'cn-northwest-1': '453252182341.dkr.ecr.cn-northwest-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'eu-central-1': '048819808253.dkr.ecr.eu-central-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'eu-north-1': '895015795356.dkr.ecr.eu-north-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'eu-south-1': '933208885752.dkr.ecr.eu-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'eu-west-1': '468650794304.dkr.ecr.eu-west-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'eu-west-2': '749857270468.dkr.ecr.eu-west-2.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'eu-west-3': '680080141114.dkr.ecr.eu-west-3.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'me-south-1': '607024016150.dkr.ecr.me-south-1.amazonaws.com/sagemaker-model-monitor-analyzer',
        #         'sa-east-1': '539772159869.dkr.ecr.sa-east-1.amazonaws.com/sagemaker-model-monitor-analyzer'
        #     },
        #     'sklearn': {
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
        #     }
        # }
        
        # Define the primary 'data' bucket
        data_bucket = s3.Bucket(
            self,
            'DataS3Bucket',
            bucket_name=f'data-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}',
            # bucket_name='data-'+cdk.Aws.REGION+'-'+cdk.Aws.ACCOUNT_ID,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True
        )

        # # Store the Bucket Name as an SSM parameter to make it accessable outside the Pipeline environment
        # bucket_parameter = ssm.StringParameter(
        #     self,
        #     'BucketNameParameter',
        #     description='Data Bucket Name',
        #     parameter_name='DataBucketName',
        #     string_value=data_bucket.bucket_name
        # )

        # Create a placeholder SSM parameter for the trained model's registry package
        package_paramter = ssm.StringParameter(
            self,
            'ModelPackageParameter',
            description='Model Package Name',
            parameter_name='ModelPackageName',
            string_value='PLACEHOLDER'
        )

        # Create a placeholder SSM paramter for the location of the 'baseline' data
        baseline_parameter = ssm.StringParameter(
            self,
            'BaselineDataParameter',
            description='Baseline Data S3 URI',
            parameter_name='BaselineDataUri',
            string_value='PLACEHOLDER'
        )
        
        # Upload the 'raw' data
        s3_deployment.BucketDeployment(
            self,
            'DeployData',
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), '..', 'data'))
            ],
            destination_bucket=data_bucket,
            destination_key_prefix='input/raw',
            retain_on_delete=False
        )
        
        # Upload the 'processing' scripts
        s3_deployment.BucketDeployment(
            self,
            'DeployScripts',
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), '..', 'scripts'))
            ],
            destination_bucket=data_bucket,
            destination_key_prefix='scripts',
            retain_on_delete=False
        )
  
        # Define the 'training/inference' container image
        model_image = ecr_assets.DockerImageAsset(
            self,
            'ModelImage',
            directory=os.path.join(os.path.dirname(__file__), '..', 'model')
        )
        
        # Define the Lambda Function to create the production 'model registry'
        registry_creator = lambda_.Function(
            self,
            'RegistryCreator',
            handler='index.lambda_handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), '..', 'lambda', 'registryCreator')),
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )
        
        # Add permission to create and delete the 'model registry'
        registry_creator.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    'sagemaker:*ModelPackage*'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*']
            )
        )
        
        # Invoke the custom resource to create the model registry
        # NOTE: A custom resource is being used here instead of 'CfnModelPackageGroup'
        #       to ensure that the model packages are deleted before deleting the group.
        cdk.CustomResource(
            self,
            'InvokeRegistryCreator',
            service_token=cr.Provider(
                self,
                'RegistryCreatorProvider',
                on_event_handler=registry_creator
            ).service_token,
            properties={
                'GroupName': group_name
            }
        )
        
        # Create a IAM Role for the Step Functions State Machine
        step_functions_role = iam.Role(
            self, 
            'StepFunctionsRole',
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal('states.amazonaws.com'),
                iam.ServicePrincipal('sagemaker.amazonaws.com')
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchLogsFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AWSLambda_FullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSageMakerFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('CloudWatchEventsFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSageMakerFeatureStoreAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonAthenaFullAccess')
            ]
        )

        # Give the Step Funcitomn role access to the model repository
        model_image.repository.grant_pull_push(step_functions_role)
        
        # Before starting the State Machine, create a failure state to catch any errors
        failure_state = sfn.Fail(self, 'Workflow Failed', cause='WorkflowFailed')
        
        # Create the Lambda Function to start teh SageMaker Experiment
        experiment_creator = lambda_.Function(
            self,
            'ExperimentCreator',
            handler='index.lambda_handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), '..', 'lambda', 'createExperiment')),
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )

        # Add permission to create the experiment
        experiment_creator.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    'sagemaker:ListExperiments',
                    'sagemaker:CreateExperiment',
                    'sagemaker:CreateTrial*',
                    'codepipeline:GetPipelineState'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*']
            )
        )

        # Define the 'create_experiment' step of the workflow
        # NOTE: The first step of the workflow is to take the input parameters and customize them
        #       based on the Pipeline execution.
        create_experiment_step = tasks.LambdaInvoke(
            self,
            'Create SageMaker Experiment',
            lambda_function=experiment_creator,
            result_path='$.createExperiment',
            payload=sfn.TaskInput.from_object(
                {
                    'modelName.$': '$.input.model_name',
                    'pipelineName.$': '$.input.pipeline_name',
                    'dataBucket.$': '$.input.data_bucket',
                    'stageName.$': '$.input.stage_name',
                    'actionName.$': '$.input.action_name'
                }
            )
        ).add_catch(failure_state, result_path='$.error')
        
        # Create the Processing Job definition
        # NOTE: The following step preprocess the raw data, using a SageMaker Processing Job.
        #       It is the Step Functions 'Task' definition and not the SageMaker Processing
        #       Job definition. 
        processing_definition = {
            'Type': 'Task',
            'Resource': 'arn:aws:states:::sagemaker:createProcessingJob.sync',
            'Parameters': {
                'ProcessingJobName.$': '$.createExperiment.Payload.processingJobName',
                'ProcessingInputs': [
                    {
                        'InputName': 'code',
                        'S3Input': {
                            'S3Uri.$': '$.createExperiment.Payload.processingCodeInput',
                            'LocalPath': '/opt/ml/processing/input/code',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File',
                            'S3DataDistributionType': 'FullyReplicated',
                            'S3CompressionType': 'None'
                        }
                    },
                    {
                        'InputName': 'data',
                        'S3Input': {
                            'S3Uri.$': '$.createExperiment.Payload.processingDataInput',
                            'LocalPath': '/opt/ml/processing/input/data',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File',
                            'S3DataDistributionType': 'FullyReplicated',
                            'S3CompressionType': 'None'
                        }
                    }
                ],
                'ProcessingOutputConfig': {
                    'Outputs': [
                        {
                            'OutputName': 'training',
                            'S3Output': {
                                'S3Uri.$': '$.createExperiment.Payload.processingTrainingOutput',
                                'LocalPath': '/opt/ml/processing/output/training',
                                'S3UploadMode': 'EndOfJob'
                            }
                        },
                        {
                            'OutputName': 'testing',
                            'S3Output': {
                                'S3Uri.$': '$.createExperiment.Payload.processingTestingOutput',
                                'LocalPath': '/opt/ml/processing/output/testing',
                                'S3UploadMode': 'EndOfJob'
                            }
                        },
                        # {
                        #     'OutputName': 'baseline',
                        #     'S3Output': {
                        #         'S3Uri.$': '$.createExperiment.Payload.processingBaselineOutput',
                        #         'LocalPath': '/opt/ml/processing/output/baseline',
                        #         'S3UploadMode': 'EndOfJob'
                        #     }
                        # }
                    ]
                },
                'AppSpecification': {
                    # 'ImageUri.$': '$.createExperiment.Payload.processingImage',
                    'ImageUri': model_image.image_uri,
                    'ContainerEntrypoint': [
                        'python3',
                        '/opt/ml/processing/input/code/preprocessing.py'
                    ]
                },
                'Environment': {
                    'MODEL_NAME.$': '$.input.model_name',
                    'AWS_REGION': cdk.Aws.REGION
                },
                'ExperimentConfig':{
                    'ExperimentName.$': '$.createExperiment.Payload.experimentName',
                    'TrialName.$': '$.createExperiment.Payload.trialName',
                    'TrialComponentDisplayName': 'Preprocessing'
                },
                'RoleArn': step_functions_role.role_arn,
                'ProcessingResources': {
                    'ClusterConfig': {
                        'InstanceCount': 1,
                        'InstanceType': 'ml.m5.xlarge',
                        'VolumeSizeInGB': 30
                    }
                },
                'StoppingCondition': {
                    'MaxRuntimeInSeconds': 600
                }
            },
            'ResultPath': '$.processingJob',
            'Catch': [
                {
                    'ErrorEquals': [
                        'States.ALL'
                    ],
                    'ResultPath': '$.error',
                    'Next': 'Workflow Failed'
                }
            ]
        }
        
        # Define the workflow pre-processing step using the above definition
        processing_step = sfn.CustomState(self, 'Data Preprocessing Job', state_json=processing_definition)
        
        """
        # Create the workflow model training step
        training_step = tasks.SageMakerCreateTrainingJob(
            self,
            'Model Training Job',
            training_job_name=sfn.JsonPath.string_at('$.createExperiment.Payload.trainingJobName'),
            # training_job_name=sfn.JsonPath.string_at('$.input.training_job_name'),
            algorithm_specification=tasks.AlgorithmSpecification(
                training_image=tasks.DockerImage.from_registry(
                    image_uri=model_image.image_uri
                )
            ),
            input_data_config=[
                tasks.Channel(
                    channel_name='training',
                    content_type='text/csv',
                    data_source=tasks.DataSource(
                        s3_data_source=tasks.S3DataSource(
                            s3_location=tasks.S3Location.from_bucket(
                                bucket=data_bucket,
                                key_prefix=sfn.JsonPath.string_at('$.createExperiment.Payload.trainingPrefix')+'input/training/'
                                # key_prefix='input/training/'
                            ),
                            s3_data_distribution_type=tasks.S3DataDistributionType.FULLY_REPLICATED,
                            s3_data_type=tasks.S3DataType.S3_PREFIX
                        )
                    )
                )
            ],
            output_data_config=tasks.OutputDataConfig(
                s3_output_location=tasks.S3Location.from_bucket(
                    bucket=data_bucket,
                    key_prefix=sfn.JsonPath.string_at('$.createExperiment.Payload.trainingPrefix')
                    # key_prefix='training_jobs/'
                )
            ),
            resource_config=tasks.ResourceConfig(
                instance_count=1,
                instance_type=ec2.InstanceType.of(ec2.InstanceClass.COMPUTE5, ec2.InstanceSize.XLARGE),
                volume_size=cdk.Size.gibibytes(30)
            ),
            role=step_functions_role,
            stopping_condition=tasks.StoppingCondition(
                max_runtime=cdk.Duration.minutes(20)
            ),
            hyperparameters={
                'epochs': '2000',
                'layers': '2',
                'dense_layer': '64',
                'batch_size': '8'
            },
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path='$.trainingJob'
        ).add_catch(failure_state, result_path='$.error')
        """

        # Create the workflow model training step
        # NOTE: This can be implemented using the 'aws_stepfunctions_tasks.SageMakerCreateTrainingJob',
        #       however, there is no current support for adding the SageMaker Experiment configuration.
        #       Therefore to add this functionality, an aws_'stepfunctions.CustomState' is used.
        training_definition = {
            'Type': 'Task',
            'Resource': 'arn:aws:states:::sagemaker:createTrainingJob.sync',
            'Parameters': {
                'TrainingJobName.$': '$.createExperiment.Payload.trainingJobName',
                'AlgorithmSpecification': {
                    'TrainingImage': model_image.image_uri,
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
                        # {
                        #     'Name': 'accuracy',
                        #     'Regex': 'accuracy: ([0-9\\.]+)'
                        # },
                        {
                            'Name': 'validation_loss',
                            'Regex': 'val_loss: ([0-9\\.]+)'
                        },
                        {
                            'Name': 'validation_mae',
                            'Regex': 'val_mae: ([0-9\\.]+)'
                        },
                        # {
                        #     'Name': 'validation_accuracy',
                        #     'Regex': 'val_accuracy: ([0-9\\.]+)'
                        # }
                    ],
                },
                'ExperimentConfig': {
                    'ExperimentName.$': '$.createExperiment.Payload.experimentName',
                    'TrialName.$': '$.createExperiment.Payload.trialName',
                    'TrialComponentDisplayName': 'Training'
                },
                'HyperParameters': {
                    'epochs': '2000',
                    'layers': '2',
                    'dense_layer': '64',
                    'batch_size': '8'
                },
                'InputDataConfig': [
                    {
                        'ChannelName': 'training',
                        'ContentType': 'text/csv',
                        'DataSource': {
                            'S3DataSource': {
                                'S3DataDistributionType': 'FullyReplicated',
                                'S3DataType': 'S3Prefix',
                                'S3Uri.$': '$.createExperiment.Payload.trainingDataInput'
                            }
                        }
                    }
                ],
                'OutputDataConfig': {
                    'S3OutputPath.$': '$.createExperiment.Payload.trainingModelOutput'
                },
                'ResourceConfig': {
                    'InstanceCount': 1,
                    'InstanceType': 'ml.m5.xlarge',
                    'VolumeSizeInGB': 30
                },
                'RoleArn': step_functions_role.role_arn,
                'StoppingCondition': {
                    'MaxRuntimeInSeconds': 3600
                }
            },
            'ResultPath': '$.trainingJob',
            'Catch': [
                {
                    'ErrorEquals': [
                        'States.ALL'
                    ],
                    'ResultPath': '$.error',
                    'Next': 'Workflow Failed'
                }
            ]
        }

        # Define the workflow training step using the above definition
        training_step = sfn.CustomState(self, 'Model Training Job', state_json=training_definition)
        
        # Create the model evaluation job definition
        evaluation_definition = {
            'Type': 'Task',
            'Resource': 'arn:aws:states:::sagemaker:createProcessingJob.sync',
            'Parameters': {
                'ProcessingJobName.$': '$.createExperiment.Payload.evaluationJobName',
                'ProcessingInputs': [
                    {
                        'InputName': 'code',
                        'S3Input': {
                            'S3Uri.$': '$.createExperiment.Payload.evaluationCodeInput',
                            'LocalPath': '/opt/ml/processing/input/code',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File',
                            'S3DataDistributionType': 'FullyReplicated',
                            'S3CompressionType': 'None'
                        }
                    },
                    {
                        'InputName': 'data',
                        'S3Input': {
                            'S3Uri.$': '$.createExperiment.Payload.evaluationDataInput',
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
                            'S3Uri.$': '$.trainingJob.ModelArtifacts.S3ModelArtifacts',
                            'LocalPath': '/opt/ml/processing/input/model',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File',
                            'S3DataDistributionType': 'FullyReplicated',
                            'S3CompressionType': 'None'
                        }
                    }
                ],
                'ProcessingOutputConfig': {
                    'Outputs': [
                        {
                            'OutputName': 'evaluation',
                            'S3Output': {
                                'S3Uri.$': '$.createExperiment.Payload.evaluationOutput',
                                'LocalPath': '/opt/ml/processing/output/evaluation',
                                'S3UploadMode': 'EndOfJob'
                            }
                        },
                        {
                            'OutputName': 'baseline',
                            'S3Output': {
                                'S3Uri.$': '$.createExperiment.Payload.processingBaselineOutput',
                                'LocalPath': '/opt/ml/processing/output/baseline',
                                'S3UploadMode': 'EndOfJob'
                            }
                        }
                    ]
                },
                'AppSpecification': {
                    'ImageUri': model_image.image_uri,
                    'ContainerEntrypoint': [
                        'python3',
                        '/opt/ml/processing/input/code/evaluation.py'
                    ]
                },
                'ExperimentConfig': {
                    'ExperimentName.$': '$.createExperiment.Payload.experimentName',
                    'TrialName.$': '$.createExperiment.Payload.trialName',
                    'TrialComponentDisplayName': 'Evaluation'
                },
                'RoleArn': step_functions_role.role_arn,
                'ProcessingResources': {
                    'ClusterConfig': {
                        'InstanceCount': 1,
                        'InstanceType': 'ml.m5.xlarge',
                        'VolumeSizeInGB': 30
                    }
                },
                'StoppingCondition': {
                    'MaxRuntimeInSeconds': 600
                }
            },
            'ResultPath': '$.evaluationJob',
            'Catch': [
                {
                    'ErrorEquals': [
                        'States.ALL'
                    ],
                    'ResultPath': '$.error',
                    'Next': 'Workflow Failed'
                }
            ]
        }
        
        # Define the workflow model evaluation step using the above definition
        evaluation_step = sfn.CustomState(self, 'Model Evaluation Job', state_json=evaluation_definition)

        # Define the Lambda function to analyze the evaluation reaults
        evaluate_results = lambda_.Function(
            self,
            'EvaluateResults',
            code=lambda_.Code.from_asset(
                os.path.join(os.path.dirname(__file__), '..', 'lambda', 'evaluateResults')
            ),
            handler='index.handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            environment={
                'PACKAGE_PARAMETER': package_paramter.parameter_name,
                'BUCKET': data_bucket.bucket_name,
                # 'KEY': 'input/evaluation/evaluation.json'
            },
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )

        # Add permissions to describe the query the 'model registry'
        evaluate_results.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    'sagemaker:*ModelPackage*'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*']
            )
        )

        # Give 'evaluate_reaults' function READ access to the data bucket and parameter store
        data_bucket.grant_read(evaluate_results)
        package_paramter.grant_read(evaluate_results)

        # Define the 'get_results' step of the workflow
        results_step = tasks.LambdaInvoke(
            self,
            'Get Evaluate Results',
            lambda_function=evaluate_results,
            result_path='$.evaluateResults',
            payload=sfn.TaskInput.from_object(
                {
                    'evaluationFile.$': '$.createExperiment.Payload.evaluationOutputFile'
                }
            )
        ).add_catch(failure_state, result_path='$.error')

        # Create the Lambda function to register the production model in the 'model registry'
        register_model = lambda_.Function(
            self,
            'RegisterModel',
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), '..', 'lambda', 'registerModel')),
            handler='index.lambda_handler',
            environment={
                'GROUP_NAME': group_name,
                'BUCKET': data_bucket.bucket_name,
                'IMAGE_URI': model_image.image_uri,
                'PACKAGE_PARAMETER': package_paramter.parameter_name,
                'BASELINE_PARAMETER': baseline_parameter.parameter_name
            },
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )

        # Add permission to add the model to the 'model registry'
        register_model.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    'sagemaker:*ModelPackage*'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*']
            )
        )

        # Give the 'register_model' funciton access to the necessary resources to update the Model Registry & SSM Parameter
        data_bucket.grant_read(register_model)
        model_image.repository.grant_pull_push(register_model)
        package_paramter.grant_write(register_model)
        baseline_parameter.grant_write(register_model)

        # Define the 'register_model_step' of the workflow
        register_model_step = tasks.LambdaInvoke(
            self,
            'Register New Production Model',
            lambda_function=register_model,
            result_path='$.registerModel',
            payload=sfn.TaskInput.from_object(
                {
                  'modelUri.$': '$.trainingJob.ModelArtifacts.S3ModelArtifacts',
                  'evaluationUri.$': '$.createExperiment.Payload.evaluationOutputFile',
                  'baselineUri.$': '$.createExperiment.Payload.baselineDataInput',
                  'executionId.$': '$.createExperiment.Payload.executionId'
                }
            )
        ).add_catch(failure_state, result_path='$.error')

        """
        # NOTE: Removing the baseline step in favor of using it in the production application to capture
        #       Model Quality.

        # Create a Baseline Processing Job definition
        baseline_definition = {
            'Type': 'Task',
            'Resource': 'arn:aws:states:::sagemaker:createProcessingJob.sync',
            'Parameters': {
                'ProcessingJobName.$': '$.createExperiment.Payload.baselineJobName',
                'ProcessingInputs': [
                    {
                        'InputName': 'baseline_dataset_input',
                        'S3Input': {
                            'S3Uri.$': '$.createExperiment.Payload.baselineDataInput',
                            'LocalPath': '/opt/ml/processing/input/baseline_dataset_input',
                            'S3DataType': 'S3Prefix',
                            'S3InputMode': 'File',
                            'S3DataDistributionType': 'FullyReplicated',
                            'S3CompressionType': 'None'
                        }
                    }
                ],
                'ProcessingOutputConfig': {
                    'Outputs': [
                        {
                            'OutputName': 'monitoring_output',
                            'S3Output': {
                                'S3Uri': f's3://{data_bucket.bucket_name}/baseline_report',
                                'LocalPath': '/opt/ml/processing/output',
                                'S3UploadMode': 'EndOfJob'
                            }
                        }
                    ]
                },
                'AppSpecification': {
                    'ImageUri.$': '$.createExperiment.Payload.baselineImage',
                },
                'Environment': {
                    "dataset_format": "{\"csv\": {\"header\": true, \"output_columns_position\": \"START\"}}",
                    "dataset_source": "/opt/ml/processing/input/baseline_dataset_input",
                    "output_path": "/opt/ml/processing/output",
                    "publish_cloudwatch_metrics": "Disabled"
                },
                'RoleArn': step_functions_role.role_arn,
                'ProcessingResources': {
                    'ClusterConfig': {
                        'InstanceCount': 1,
                        'InstanceType': 'ml.m5.xlarge',
                        'VolumeSizeInGB': 30
                    }
                },
                'StoppingCondition': {
                    'MaxRuntimeInSeconds': 1800
                }
            },
            'ResultPath': '$.baselineJob'
        }

        # Define the workflow baseline step using the above definition
        baseline_step = sfn.CustomState(self, 'Suggest Baseline', state_json=baseline_definition)

        # Define a parallel step to execute the baseline and model registry update simultaneously
        parallel_step = sfn.Parallel(self, 'Finalize Production Model')
        parallel_step.branch(baseline_step)
        parallel_step.branch(register_model_step)
        parallel_step.add_catch(failure_state, result_path='$.error')
        """

        # Create the model version comparison 'success' and 'failure' states
        # NOTE: Model Version comparison failure will not result in a workflow failure, we simply
        #       do not register the version. thus always ensuring that only the best model is
        #       registered.
        version_success_state = sfn.Pass(self, 'Yes').next(register_model_step)
        version_failed_state = sfn.Pass(self, 'No')

        # Define the 'choice' brnach to determine if the current model is better than the production model version
        version_choice = sfn.Choice(self, 'Is this Version an Improvement?')
        version_choice.when(sfn.Condition.string_equals('$.evaluateResults.Payload.improved', 'TRUE'), version_success_state)
        version_choice.otherwise(version_failed_state)


        # Create the model evaluation 'success' and 'failure' states
        quality_success_state = sfn.Pass(self, 'Model Below Quality Theshold').next(version_choice)
        quality_failed_state = sfn.Fail(self, 'Model Above Quality Threshold')

        # Define the 'choice' branch to determine model quality threshold success or failure
        # NOTE: Lambda's results are in the attribute 'Payload'
        quality_choice = sfn.Choice(self, 'Evaluate Model Quality Threshold')
        quality_choice.when(sfn.Condition.number_less_than('$.evaluateResults.Payload.rmse', threshold), quality_success_state)
        quality_choice.otherwise(quality_failed_state)

        # Create the workflow
        workflow_definition = create_experiment_step.next(
            processing_step
        ).next(
            training_step
        ).next(
            evaluation_step
        ).next(
            results_step
        ).next(
            quality_choice
        )

        # Defeint the State Machine
        workflow = sfn.StateMachine(
            self,
            'MLWorkflow',
            definition=workflow_definition,
            role=step_functions_role,
            timeout=cdk.Duration.minutes(60)
        )

        # Generate the outputs used by additional stages within the pipeline
        self.sfn_output = cdk.CfnOutput(self, 'StateMachineArn', value=workflow.state_machine_arn)
        self.data_bucket = cdk.CfnOutput(self, 'DataBucket', value=data_bucket.bucket_name)#.to_string()
