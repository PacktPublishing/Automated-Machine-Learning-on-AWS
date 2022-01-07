import aws_cdk as cdk
import aws_cdk.aws_codecommit as codecommit
import aws_cdk.aws_s3 as s3
import aws_cdk.pipelines as pipelines
import aws_cdk.aws_ssm as ssm
from constructs import Construct

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
            bucket_name=f"data-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}",
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

        pipeline = pipelines.CodePipeline(
            self,
            "Application-Pipeline",
            pipeline_name="ACME-WebApp-Pipeline",
            self_mutation=False,
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