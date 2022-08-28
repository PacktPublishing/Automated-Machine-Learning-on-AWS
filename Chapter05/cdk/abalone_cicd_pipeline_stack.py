import os
import aws_cdk as cdk
import aws_cdk.aws_codecommit as codecommit
import aws_cdk.aws_codepipeline as codepipeline
import aws_cdk.aws_codepipeline_actions as pipeline_actions
import aws_cdk.aws_codebuild as codebuild
import aws_cdk.aws_iam as iam
import aws_cdk.aws_ecr as ecr
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_s3_deployment as s3_deployment
from constructs import Construct

class PipelineStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, *, model_name: str=None, repo_name: str=None, cdk_version: str=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        sagemaker_role = iam.Role(
            self,
            "SageMakerBuildRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("sagemaker.amazonaws.com")
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")
            ]
        )
        
        code_repo = codecommit.Repository.from_repository_name(
            self,
            "PipelineRepo",
            repo_name
        )
        
        container_repo = ecr.Repository(
            self,
            "ModelRepo",
            repository_name=model_name,
            removal_policy=cdk.RemovalPolicy.DESTROY
        )
        container_repo.grant_pull_push(sagemaker_role)
        
        s3_bucket = s3.Bucket(
            self,
            "PipelineBucket",
            bucket_name=f"{repo_name}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True
        )
        s3_bucket.grant_read_write(sagemaker_role)
        
        s3_deployment.BucketDeployment(
            self,
            "DeployData",
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), '../artifacts/data'))
            ],
            destination_bucket=s3_bucket,
            destination_key_prefix="data",
            retain_on_delete=False
        )

        container_build = codebuild.PipelineProject(
            self,
            "ContainerBuild",
            project_name="ModelContainerBuild",
            description="CodeBuild Project for building the Model Container",
            environment=codebuild.BuildEnvironment(
                privileged=True,
                build_image=codebuild.LinuxBuildImage.STANDARD_4_0
            ),
            environment_variables={
                "AWS_DEFAULT_REGION": codebuild.BuildEnvironmentVariable(
                    value=cdk.Aws.REGION
                ),
                "AWS_ACCOUNT_ID": codebuild.BuildEnvironmentVariable(
                    value=cdk.Aws.ACCOUNT_ID
                ),
                "IMAGE_REPO_NAME": codebuild.BuildEnvironmentVariable(
                    value=container_repo.repository_uri
                ),
                "IMAGE_TAG": codebuild.BuildEnvironmentVariable(
                    value="latest"
                )
            },
            build_spec=codebuild.BuildSpec.from_object(
                dict(
                    version="0.2",
                    phases=dict(
                        pre_build= dict(
                            commands=[
                                "echo Logging in to Amazon ECR...",
                                "$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION)",
                                "$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION --registry-ids 763104351884)",
                                "$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION --registry-ids 217643126080)",
                                "$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION --registry-ids 727897471807)",
                                "$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION --registry-ids 626614931356)",
                                "$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION --registry-ids 683313688378)",
                                "$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION --registry-ids 520713654638)",
                                "$(aws ecr get-login --no-include-email --region $AWS_DEFAULT_REGION --registry-ids 462105765813)"
                            ]
                        ),
                        build=dict(
                            commands=[
                                "echo Build started on `date`",
                                "echo Building the Docker image...",
                                "docker build -t $IMAGE_REPO_NAME:$IMAGE_TAG --build-arg REGION=$AWS_DEFAULT_REGION ."
                            ]
                        ),
                        post_build=dict(
                            commands=[
                                "echo Build completed on `date`",
                                "echo Pushing the Docker image...",
                                "docker push $IMAGE_REPO_NAME:$IMAGE_TAG"
                            ]
                        )
                    )
                )
            )
        )
        container_build.role.add_to_policy(
            iam.PolicyStatement(
                resources=[
                    "arn:aws:ecr:*:763104351884:repository/*",
                    "arn:aws:ecr:*:217643126080:repository/*",
                    "arn:aws:ecr:*:727897471807:repository/*",
                    "arn:aws:ecr:*:626614931356:repository/*",
                    "arn:aws:ecr:*:683313688378:repository/*",
                    "arn:aws:ecr:*:520713654638:repository/*",
                    "arn:aws:ecr:*:462105765813:repository/*"
                ],
                actions=[
                    "ecr:BatchGetImage",
                    "ecr:GetDownloadUrlForLayer"
                ],
                effect=iam.Effect.ALLOW
            )
        )
        container_build.role.add_to_policy(
            iam.PolicyStatement(
                resources=["*"],
                actions=[
                    "ecr:GetAuthorizationToken",
                ],
                effect=iam.Effect.ALLOW
            )
        )
        s3_bucket.grant_read_write(container_build)
        container_repo.grant_pull_push(container_build)
        
        data_build = codebuild.PipelineProject(
            self,
            "DataBuild",
            project_name="TrainingDataBuild",
            description="CodeBuild Project to create training, validation and testing data",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0
            ),
            environment_variables={
                "IMAGE_URI": codebuild.BuildEnvironmentVariable(
                    value=container_repo.repository_uri
                ),
                "BUCKET_NAME": codebuild.BuildEnvironmentVariable(
                    value=s3_bucket.bucket_name
                ),
                "ROLE_ARN": codebuild.BuildEnvironmentVariable(
                    value=sagemaker_role.role_arn
                ),
                "PIPELINE_NAME": codebuild.BuildEnvironmentVariable(
                    value=repo_name
                ),
                "MODEL_NAME": codebuild.BuildEnvironmentVariable(
                    value=model_name
                )
            },
            build_spec=codebuild.BuildSpec.from_object(
                dict(
                    version="0.2",
                    phases={
                        "install": {
                            "runtime-versions": {"python": 3.8},
                            "commands": [
                                "echo Updating build environment",
                                "python -m pip install --upgrade pip",
                                "python -m pip install --upgrade --force-reinstall boto3"
                            ]
                        },
                        "build": {
                            "commands": [
                                "echo Build started on `date`",
                                "python ./artifacts/scripts/build.py preprocess"
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "echo Build completed on `date`"
                            ]
                        }
                    }
                )
            )
        )
        data_build.role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "sagemaker:DescribeProcessingJob",
                    "sagemaker:CreateProcessingJob",
                    "codepipeline:GetPipelineState"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        s3_bucket.grant_read_write(data_build)
        sagemaker_role.grant_pass_role(data_build)

        model_build = codebuild.PipelineProject(
            self,
            "ModelBuild",
            project_name="ModelTrainingBuild",
            description="CodeBuild Project to train a SageMaker Model",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0
            ),
            environment_variables={
                "IMAGE_URI": codebuild.BuildEnvironmentVariable(
                    value=container_repo.repository_uri
                ),
                "BUCKET_NAME": codebuild.BuildEnvironmentVariable(
                    value=s3_bucket.bucket_name
                ),
                "ROLE_ARN": codebuild.BuildEnvironmentVariable(
                    value=sagemaker_role.role_arn
                ),
                "PIPELINE_NAME": codebuild.BuildEnvironmentVariable(
                    value=repo_name
                ),
                "MODEL_NAME": codebuild.BuildEnvironmentVariable(
                    value=model_name
                )
            },
            build_spec=codebuild.BuildSpec.from_object(
                dict(
                    version="0.2",
                    phases={
                        "install": {
                            "runtime-versions": {"python": 3.8},
                            "commands": [
                                "echo Updating build environment",
                                "python -m pip install --upgrade pip",
                                "python -m pip install --upgrade --force-reinstall boto3"
                            ]
                        },
                        "build": {
                            "commands": [
                                "echo Build started on `date`",
                                "python ./artifacts/scripts/build.py train"
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "echo Build completed on `date`"
                            ]
                        }
                    }
                )
            )
        )
        model_build.role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "sagemaker:DescribeTrainingJob",
                    "sagemaker:CreateTrainingJob",
                    "codepipeline:GetPipelineState"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        s3_bucket.grant_read_write(model_build)
        sagemaker_role.grant_pass_role(model_build)

        evaluation_build = codebuild.PipelineProject(
            self,
            "Evaluationuild",
            project_name="ModelEvaluationBuild",
            description="CodeBuild Project to evaluate the traing model",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0
            ),
            environment_variables={
                "IMAGE_URI": codebuild.BuildEnvironmentVariable(
                    value=container_repo.repository_uri
                ),
                "BUCKET_NAME": codebuild.BuildEnvironmentVariable(
                    value=s3_bucket.bucket_name
                ),
                "ROLE_ARN": codebuild.BuildEnvironmentVariable(
                    value=sagemaker_role.role_arn
                ),
                "PIPELINE_NAME": codebuild.BuildEnvironmentVariable(
                    value=repo_name
                ),
                "MODEL_NAME": codebuild.BuildEnvironmentVariable(
                    value=model_name
                )
            },
            build_spec=codebuild.BuildSpec.from_object(
                dict(
                    version="0.2",
                    phases={
                        "install": {
                            "runtime-versions": {"python": 3.8},
                            "commands": [
                                "echo Updating build environment",
                                "python -m pip install --upgrade pip",
                                "python -m pip install --upgrade --force-reinstall boto3"
                            ]
                        },
                        "build": {
                            "commands": [
                                "echo Build started on `date`",
                                "python ./artifacts/scripts/build.py evaluate"
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "echo Build completed on `date`"
                            ]
                        }
                    }
                )
            )
        )
        evaluation_build.role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "sagemaker:DescribeTrainingJob",
                    "sagemaker:DescribeProcessingJob",
                    "sagemaker:CreateProcessingJob",
                    "codepipeline:GetPipelineState"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        sagemaker_role.grant_pass_role(evaluation_build)
        s3_bucket.grant_read_write(evaluation_build)

        deployment_build = codebuild.PipelineProject(
            self,
            "DeploymentBuild",
            project_name="DeploymentBuild",
            description="CodeBuild Project to Synthesize a SageMaker Endpoint CloudFormation Template",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0
            ),
            environment_variables={
                "IMAGE_URI": codebuild.BuildEnvironmentVariable(
                    value=container_repo.repository_uri
                ),
                "BUCKET_NAME": codebuild.BuildEnvironmentVariable(
                    value=s3_bucket.bucket_name
                ),
                "ROLE_ARN": codebuild.BuildEnvironmentVariable(
                    value=sagemaker_role.role_arn
                ),
                "PIPELINE_NAME": codebuild.BuildEnvironmentVariable(
                    value=repo_name
                ),
                "MODEL_NAME": codebuild.BuildEnvironmentVariable(
                    value=model_name
                )
            },
            build_spec=codebuild.BuildSpec.from_object(
                dict(
                    version="0.2",
                    phases={
                        "install": {
                            "runtime-versions": {
                                "python": 3.8,
                                "nodejs": 14
                            },
                            "commands": [
                                "echo Updatimg build environment",
                                "npm install aws-cdk@{}".format(cdk_version),
                                "python -m pip install --upgrade pip",
                                "python -m pip install -r requirements.txt"
                            ]
                        },
                        "build": {
                            "commands": [
                                "echo Synthesizing cdk template",
                                "npx cdk synth -o output"
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "python ./artifacts/scripts/deploy.py"
                            ]
                        }
                    },
                    artifacts={
                        "base-directory": "output",
                        "files": [
                            "EndpointStack.template.json",
                            "params.json"
                        ]
                    }
                )
            )
        )
        deployment_build.role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "sagemaker:DescribeTrainingJob",
                    "codepipeline:GetPipelineState",
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        sagemaker_role.grant_pass_role(deployment_build)
        s3_bucket.grant_read_write(deployment_build)
        
        main_source_output = codepipeline.Artifact()
        model_source_output = codepipeline.Artifact()
        deployment_build_output = codepipeline.Artifact("DeploymentBuildOutput")
        pipeline = codepipeline.Pipeline(
            self,
            "Pipeline",
            pipeline_name=repo_name,
            artifact_bucket=s3_bucket,
            stages=[
                codepipeline.StageProps(
                    stage_name="Source",
                    actions=[
                        pipeline_actions.CodeCommitSourceAction(
                            action_name="MainSource",
                            branch="main",
                            repository=code_repo,
                            output=main_source_output
                        ),
                        pipeline_actions.CodeCommitSourceAction(
                            action_name="ModelSource",
                            branch="model",
                            repository=code_repo,
                            output=model_source_output
                        )
                    ]
                ),
                codepipeline.StageProps(
                    stage_name="Build",
                    actions=[
                        pipeline_actions.CodeBuildAction(
                            action_name="ContainerBuild",
                            project=container_build,
                            input=model_source_output,
                            run_order=1
                        ),
                        pipeline_actions.CodeBuildAction(
                            action_name="Preprocess",
                            project=data_build,
                            input=main_source_output,
                            run_order=2
                        ),
                        pipeline_actions.CodeBuildAction(
                            action_name="Train",
                            project=model_build,
                            input=main_source_output,
                            run_order=3
                        ),
                        pipeline_actions.CodeBuildAction(
                            action_name="Evaluate",
                            project=evaluation_build,
                            input=main_source_output,
                            run_order=4
                        )
                    ]
                ),
                codepipeline.StageProps(
                    stage_name="Approval",
                    actions=[
                        pipeline_actions.ManualApprovalAction(
                            action_name="EvaluationApproval",
                            additional_information="Is the Model Ready for Production?"
                        )
                    ]
                ),
                codepipeline.StageProps(
                    stage_name="Deploy",
                    actions=[
                        pipeline_actions.CodeBuildAction(
                            action_name="DeploymentBuild",
                            project=deployment_build,
                            input=main_source_output,
                            outputs=[deployment_build_output],
                            run_order=1
                        ),
                        pipeline_actions.CloudFormationCreateUpdateStackAction(
                            action_name="DeployEndpoint",
                            stack_name="EndpointStack",
                            template_path=deployment_build_output.at_path(
                                "EndpointStack.template.json"
                            ),
                            admin_permissions=True,
                            parameter_overrides={
                                "ImageUri": deployment_build_output.get_param("params.json", "ImageUri"),
                                "ExecutionId": deployment_build_output.get_param("params.json", "ExecutionId"),
                                "BucketName": deployment_build_output.get_param("params.json", "BucketName"),
                                "ModelUri": deployment_build_output.get_param("params.json", "ModelUri"),
                                "ExecutionRole": deployment_build_output.get_param("params.json", "ExecutionRole")
                            },
                            extra_inputs=[deployment_build_output],
                            run_order=2
                        )
                    ]
                )
            ]
        )
        s3_bucket.grant_read_write(pipeline.role)