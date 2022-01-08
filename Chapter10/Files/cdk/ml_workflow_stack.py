import os
import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3_deployment
import aws_cdk.aws_ecr as ecr
import aws_cdk.aws_iam as iam
import aws_cdk.aws_ecr_assets as ecr_assets
import aws_cdk.aws_lambda as lambda_
import aws_cdk.custom_resources as cr
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
import aws_cdk.aws_ssm as ssm
from constructs import Construct


class MLWorkflowStack(cdk.Stack):

    def __init__(self, scope: Construct, id: str, *, group_name: str=None, threshold: float=None, data_bucket_name: str=None, feature_group_name: str=None, **kwargs):
        super().__init__(scope, id, **kwargs)

        data_bucket = s3.Bucket.from_bucket_name(
            self,
            "Data-Bucket",
            bucket_name=data_bucket_name
        )
        s3_deployment.BucketDeployment(
            self,
            "Deploy-Scripts",
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), "../../scripts"))
            ],
            destination_bucket=data_bucket,
            destination_key_prefix="scripts",
            retain_on_delete=False
        )

        package_paramter = ssm.StringParameter(
            self,
            "Model-Package-Paramater",
            description="Model Package Name",
            parameter_name="ModelPackageName",
            string_value="PLACEHOLDER"
        )

        baseline_paramater = ssm.StringParameter(
            self,
            "Baseline-Data-Paramater",
            description="Baseline Data S3 URI",
            parameter_name="BaselineDataUri",
            string_value="PLACEHOLDER"
        )

        model_image = ecr_assets.DockerImageAsset(
            self,
            "Model-Image",
            directory=os.path.join(os.path.dirname(__file__), "../../model")
        )

        registry_creator = lambda_.Function(
            self,
            "Registry-Creator",
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), "../../lambda/registryCreator")),
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )
        registry_creator.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "sagemaker:*ModelPackage*"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        cdk.CustomResource(
            self,
            "Invoke-Registry-Creator",
            service_token=cr.Provider(
                self,
                "Registry-Creator-Provider",
                on_event_handler=registry_creator
            ).service_token,
            properties={
                "GroupName": group_name
            }
        )

        step_functions_role = iam.Role(
            self,
            "StepFunctions-Role",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("states.amazonaws.com"),
                iam.ServicePrincipal("sagemaker.amazonaws.com")
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSLambda_FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchEventsFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFeatureStoreAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
            ]
        )
        model_image.repository.grant_pull_push(step_functions_role)

        experiment_creator = lambda_.Function(
            self,
            "Experiment-Creator",
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), "../../lambda/createExperiment")),
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )
        experiment_creator.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "sagemaker:ListExperiments",
                    "sagemaker:CreateExperiment",
                    "sagemaker:CreateTrial*",
                    "codepipeline:GetPipelineState"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )

        evaluate_results = lambda_.Function(
            self,
            "Evaluate-Results",
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), "../../lambda/evaluateResults")),
            environment={
                "PACKAGE_PARAMETER": package_paramter.parameter_name,
                "BUCKET": data_bucket.bucket_name
            },
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )
        evaluate_results.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "sagemaker:*ModelPackage*"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        data_bucket.grant_read(evaluate_results)
        package_paramter.grant_read(evaluate_results)

        register_model = lambda_.Function(
            self,
            "Register-Model",
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), "../../lambda/registerModel")),
            environment={
                "GROUP_NAME": group_name,
                "BUCKET": data_bucket.bucket_name,
                "IMAGE_URI": model_image.image_uri,
                "PACKAGE_PARAMETER": package_paramter.parameter_name,
                "BASELINE_PARAMETER": baseline_paramater.parameter_name
            },
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )
        register_model.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "sagemaker:*ModelPackage*"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        data_bucket.grant_read(register_model)
        model_image.repository.grant_pull_push(register_model)
        package_paramter.grant_write(register_model)
        baseline_paramater.grant_write(register_model)

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
                    ]
                },
                'AppSpecification': {
                    'ImageUri': model_image.image_uri,
                    'ContainerEntrypoint': [
                        'python3',
                        '/opt/ml/processing/input/code/preprocessing.py'
                    ]
                },
                'Environment': {
                    'MODEL_NAME.$': '$.input.model_name',
                    'AWS_REGION': cdk.Aws.REGION,
                    'FEATURE_GROUP_NAME': feature_group_name
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
                        {
                            'Name': 'validation_loss',
                            'Regex': 'val_loss: ([0-9\\.]+)'
                        },
                        {
                            'Name': 'validation_mae',
                            'Regex': 'val_mae: ([0-9\\.]+)'
                        },
                    ],
                },
                'ExperimentConfig': {
                    'ExperimentName.$': '$.createExperiment.Payload.experimentName',
                    'TrialName.$': '$.createExperiment.Payload.trialName',
                    'TrialComponentDisplayName': 'Training'
                },
                'HyperParameters': {
                    'epochs': '200',
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

        failure_state = sfn.Fail(self, "Workflow Failed", cause="WorkflowFailed")

        create_experiment_step = tasks.LambdaInvoke(
            self,
            "Create SageMaker Experiment",
            lambda_function=experiment_creator,
            result_path="$.createExperiment",
            payload=sfn.TaskInput.from_object(
                {
                    "modelName.$": "$.input.model_name",
                    "pipelineName.$": "$.input.pipeline_name",
                    "dataBucket.$": "$.input.data_bucket",
                    "stageName.$": "$.input.stage_name",
                    "actionName.$": "$.input.action_name"
                }
            )
        ).add_catch(failure_state, result_path="$.error")

        processing_step = sfn.CustomState(self, "Data Preprocessing Job", state_json=processing_definition)

        training_step = sfn.CustomState(self, "Model Training Job", state_json=training_definition)

        evaluation_step = sfn.CustomState(self, "Model Evaluation Job", state_json=evaluation_definition)

        results_step = tasks.LambdaInvoke(
            self,
            "Get Evaluate Results",
            lambda_function=evaluate_results,
            result_path="$.evaluateResults",
            payload=sfn.TaskInput.from_object(
                {
                    "evaluationFile.$": "$.createExperiment.Payload.evaluationOutputFile"
                }
            )
        ).add_catch(failure_state, result_path="$.error")

        register_model_step = tasks.LambdaInvoke(
            self,
            "Register New Production Model",
            lambda_function=register_model,
            result_path="$.registerModel",
            payload=sfn.TaskInput.from_object(
                {
                  "modelUri.$": "$.trainingJob.ModelArtifacts.S3ModelArtifacts",
                  "evaluationUri.$": "$.createExperiment.Payload.evaluationOutputFile",
                  "baselineUri.$": "$.createExperiment.Payload.baselineDataInput",
                  "executionId.$": "$.createExperiment.Payload.executionId"
                }
            )
        ).add_catch(failure_state, result_path="$.error")

        version_success_state = sfn.Pass(self, "Yes").next(register_model_step)

        version_failed_state = sfn.Pass(self, "No")

        version_choice = sfn.Choice(self, "Is this Version an Improvement?")

        version_choice.when(sfn.Condition.string_equals("$.evaluateResults.Payload.improved", "TRUE"), version_success_state)

        version_choice.otherwise(version_failed_state)

        quality_success_state = sfn.Pass(self, "Model Below Quality Theshold").next(version_choice)

        quality_failed_state = sfn.Fail(self, "Model Above Quality Threshold")

        quality_choice = sfn.Choice(self, "Evaluate Model Quality Threshold")

        quality_choice.when(sfn.Condition.number_less_than("$.evaluateResults.Payload.rmse", threshold), quality_success_state)

        quality_choice.otherwise(quality_failed_state)

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

        workflow = sfn.StateMachine(
            self,
            'MLWorkflow',
            definition=workflow_definition,
            role=step_functions_role,
            timeout=cdk.Duration.minutes(60)
        )
        
        self.sfn_output = cdk.CfnOutput(self, "StateMachine-Arn", value=workflow.state_machine_arn)
