import aws_cdk as cdk
import aws_cdk.aws_codecommit as codecommit
import aws_cdk.aws_s3 as s3
import aws_cdk.pipelines as pipelines
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_ecr as ecr
import aws_cdk.aws_iam as iam
from constructs import Construct


from .stacks.ml_workflow_stack import MLWorkflowStack
from .stacks.test_application_stack import TestApplicaitonStack
from .stacks.production_application_stack import ProductionApplicaitonStack
from .stacks.data_workflow_stack import DataWorkflowStack

class MLWorkflowStage(cdk.Stage):
    
    def __init__(self, scope: Construct, id: str, *, group_name: str, threhold: float, data_bucket_name: str, feature_group_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        ml_workflow_stack = MLWorkflowStack(
            self,
            "MLWorkflowStack",
            group_name=group_name,
            threshold=threhold,
            data_bucket_name=data_bucket_name,
            feature_group_name=feature_group_name
        )
        self.sfn_arn = ml_workflow_stack.sfn_output

class TestApplicationStage(cdk.Stage):

    def __init__(self, scope: Construct, id: str, *, model_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        test_stack = TestApplicaitonStack(self, "TestApplicaitonStack", model_name=model_name)
        self.cdn_output = test_stack.cdn_output
        self.api_output = test_stack.api_output


class ProductionApplicationStage(cdk.Stage):
    def __init__(self, scope: Construct, id: str, *, model_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        production_stack = ProductionApplicaitonStack(self, "ProdApplicationStack", model_name=model_name)
        self.cdn_output = production_stack.cdn_output
        self.api_output = production_stack.api_output


class DataWorkflowStage(cdk.Stage):
    def __init__(self, scope: Construct, id: str, *, airflow_environment_name: str, data_bucket_name: str, pipeline_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)
        data_workflow_stack = DataWorkflowStack(self, "DataWorkflowStack", airflow_environment_name=airflow_environment_name, data_bucket_name=data_bucket_name, pipeline_name=pipeline_name)


class PipelineStack(cdk.Stack):

    def __init__(self, scope: Construct, id: str, *, model_name: str=None, group_name: str=None, repo_name: str=None, feature_group: str=None, threshold: float=None, cdk_version: str=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.code_repo = codecommit.Repository(
            self,
            "Source-Repository",
            repository_name=repo_name,
            description="ACME Web Application Source Code Repository"
        )
        cdk.CfnOutput(
            self,
            "Clone-URL",
            description="CodeCommit Clone URL",
            value=self.code_repo.repository_clone_url_http
        )

        self.data_bucket = s3.Bucket(
            self,
            "Data-Bucket",
            bucket_name=f"data-{self.region}-{self.account}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True
        )

        ssm.StringParameter(
            self,
            "Data-Bucket-Parameter",
            parameter_name="DataBucket",
            description="SSM Parameter for the S3 Data Bucket Name",
            string_value=self.data_bucket.bucket_name
        )

        ssm.StringParameter(
            self,
            "Feature-Group-Parameter",
            parameter_name="FeatureGroup",
            description="SSM Paramater for the SageMaker Feature Store group",
            string_value=feature_group
        )

        source_artifact = pipelines.CodePipelineSource.code_commit(
            repository=self.code_repo,
            branch="main"
        )

        ml_workflow_stage = MLWorkflowStage(
            self,
            "Build-MLWorkflow",
            data_bucket_name=self.data_bucket.bucket_name,
            group_name=group_name,
            threhold=threshold,
            feature_group_name=feature_group
        )

        test_stage = TestApplicationStage(
            self,
            "Test-Deployment",
            model_name=model_name
        )

        prod_stage = ProductionApplicationStage(
            self,
            "Production-Deployment",
            model_name=model_name
        )

        data_workflow_stage = DataWorkflowStage(
            self,
            "Build-DataWorkflow",
            airflow_environment_name="acme-airflow-environment",
            data_bucket_name=self.data_bucket.bucket_name,
            pipeline_name="ACME-WebApp-Pipeline"
        )

        pipeline = pipelines.CodePipeline(
            self,
            "Application-Pipeline",
            pipeline_name="ACME-WebApp-Pipeline",
            self_mutation=True,
            cli_version=cdk_version,
            synth=pipelines.ShellStep(
                "Synth",
                input=source_artifact,
                commands=[
                    "printenv",
                    f"npm install -g aws-cdk@{cdk_version}",
                    "python -m pip install --upgrade pip",
                    "pip install -r requirements.txt",
                    "cdk synth"
                ]
            )
        )
        pipeline.add_stage(
            ml_workflow_stage,
            post=[
                pipelines.CodeBuildStep(
                    "Execute-MLWorkflow",
                    input=source_artifact,
                    commands=[
                        "python3 ./scripts/invoke.py"
                    ],
                    env_from_cfn_outputs={
                        "STATEMACHINE_ARN": ml_workflow_stage.sfn_arn
                    },
                    env={
                        "MODEL_NAME": model_name,
                        "PIPELINE_NAME": "ACME-WebApp-Pipeline",
                        "STAGE_NAME": "Build-MLWorkflow",
                        "ACTION_NAME": "Execute-MLWorkflow",
                        "DATA_BUCKET": self.data_bucket.bucket_name
                    },
                    role_policy_statements=[
                        iam.PolicyStatement(
                            actions=[
                                "states:ListStateMachine",
                                "states:DescribeStateMachine",
                                "states:DescribeExecution",
                                "states:ListExecutions",
                                "states:GetExecutionHistory",
                                "states:StartExecution",
                                "states:StopExecution"
                            ],
                            effect=iam.Effect.ALLOW,
                            resources=["*"]
                        )
                    ]
                )
            ]
        )
        pipeline.add_stage(
            test_stage,
            post=[
                pipelines.ShellStep(
                    "System-Tests",
                    input=source_artifact,
                    commands=[
                        "pip install -r ./tests/requirements.txt",
                        "pytest ./tests/system_tests.py"
                    ],
                    env_from_cfn_outputs={
                        "WEBSITE_URL": test_stage.cdn_output,
                        "API_URL": test_stage.api_output
                    }
                )
            ]
        )
        pipeline.add_stage(prod_stage)
        pipeline.add_stage(data_workflow_stage)
