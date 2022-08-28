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
import aws_cdk.aws_ssm as ssm
from constructs import Construct

class PipelineStack(cdk.Stack):
    def __init__(self, scope: Construct, id: str, *, model_name: str=None, repo_name: str=None, cdk_version: str=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        code_repo = codecommit.Repository.from_repository_name(
            self,
            "PipelineSourceRepo",
            repository_name=repo_name
        )

        workflow_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "codepipeline:GetPipelineState",
                        "lambda:InvokeFunction",
                        "lambda:UpdateFunctionCode",
                        "lambda:CreateFunction",
                        "states:CreateStateMachine",
                        "states:UpdateStateMachine",
                        "states:DeleteStateMachine",
                        "states:DescribeStateMachine",
                        "states:StartExecution"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "iam:PassRole"
                    ],
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "events:PutTargets",
                        "events:PutRule",
                        "events:DescribeRule"
                    ],
                    "Resource": "*"
                }
            ]
        }

        workflow_role = iam.Role(
            self,
            "WorkflowExecutionRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("codebuild.amazonaws.com")
            )
        )
        workflow_role.attach_inline_policy(
            iam.Policy(
                self,
                "WorkflowRoleInlinePolicy",
                document=iam.PolicyDocument.from_json(workflow_policy_document)
            )
        )
        workflow_role.assume_role_policy.add_statements(
            iam.PolicyStatement(
                actions=[
                    "sts:AssumeRole"
                ],
                effect=iam.Effect.ALLOW,
                principals=[
                    iam.ServicePrincipal("lambda.amazonaws.com"),
                    iam.ServicePrincipal("sagemaker.amazonaws.com"),
                    iam.ServicePrincipal("states.amazonaws.com")
                ]
            )
        )
        workflow_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess")
        )

        workflow_role_param = ssm.StringParameter(
            self,
            "WorkflowRoleParameter",
            description="Step Functions Workflow Execution Role ARN",
            parameter_name="WorkflowRoleParameter",
            string_value=workflow_role.role_arn
        )
        workflow_role_param.grant_read(workflow_role)
        
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
        
        s3_bucket = s3.Bucket(
            self,
            "PipelineBucket",
            bucket_name=f"{repo_name}-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True
        )
        s3_bucket.grant_read_write(sagemaker_role)
        s3_bucket.grant_read_write(workflow_role)
        
        s3_bucket_param = ssm.StringParameter(
            self,
            "PipelineBucketParameter",
            description="Pipeline Bucket Name",
            parameter_name="PipelineBucketName",
            string_value=s3_bucket.bucket_name
        )

        s3_deployment.BucketDeployment(
            self,
            "DeployData",
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), '../artifacts/data'))
            ],
            destination_bucket=s3_bucket,
            destination_key_prefix="abalone_data/raw",
            retain_on_delete=False
        )
        
        workflow_build = codebuild.Project(
            self,
            "WorkflowBuildProject",
            project_name="WorkflowBuildProject",
            description="CodeBuild Project for Building and Executing the ML Workflow",
            role=workflow_role,
            source=codebuild.Source.code_commit(
                repository=code_repo
            ),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0
            ),
            environment_variables={
                "PIPELINE_NAME": codebuild.BuildEnvironmentVariable(
                    value=repo_name
                ),
                "MODEL_NAME": codebuild.BuildEnvironmentVariable(
                    value=model_name
                ),
                "BUCKET_NAME": codebuild.BuildEnvironmentVariable(
                    value=s3_bucket.bucket_name
                )
            }
        )

        deployment_build = codebuild.PipelineProject(
            self,
            "DeploymentBuild",
            project_name="DeploymentBuild",
            description="CodeBuild Project to Synthesize a SageMaker Endpoint CloudFormation Template",
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0
            ),
            environment_variables={
                "BUCKET_NAME": codebuild.BuildEnvironmentVariable(
                    value=s3_bucket.bucket_name
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
                            action_name="BuildModel",
                            project=workflow_build,
                            input=model_source_output,
                            run_order=1
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
                                "ExecutionId": deployment_build_output.get_param("params.json", "ExecutionId"),
                                "BucketName": deployment_build_output.get_param("params.json", "BucketName")
                            },
                            extra_inputs=[deployment_build_output],
                            run_order=2
                        )
                    ]
                )
            ]
        )
        s3_bucket.grant_read_write(pipeline.role)