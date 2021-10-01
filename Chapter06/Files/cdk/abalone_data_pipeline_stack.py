import os
import aws_cdk.aws_codecommit as codecommit
import aws_cdk.aws_codebuild as codebuild
import aws_cdk.core as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_s3_deployment as s3_deployment
import aws_cdk.aws_iam as iam
import aws_cdk.aws_glue as glue
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_events_targets as targets

class DataPipelineStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, id: str, *, airflow_environment_name: str=None, model_name: str=None, repo_name: str=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        
        code_repo = codecommit.Repository.from_repository_name(
            self,
            "SourceRepository",
            repository_name=repo_name
        )

        data_bucket = s3.Bucket(
            self,
            "AirflowDataBucket",
            bucket_name=f"{model_name}-data-{cdk.Aws.REGION}-{cdk.Aws.ACCOUNT_ID}",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            auto_delete_objects=True,
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True
        )
        ssm.StringParameter(
            self,
            "DataBucketParameter",
            description="Airflow Data Bucket Name",
            parameter_name="AirflowDataBucket",
            string_value=data_bucket.bucket_name
        )

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
        data_bucket.grant_read_write(sagemaker_role)
        ssm.StringParameter(
            self,
            "SageMakerRoleParameter",
            description="SageMaker Role ARN",
            parameter_name="SageMakerRoleARN",
            string_value=sagemaker_role.role_arn
        )

        analyze_results_lambda = lambda_.Function(
            self,
            "AnalyzeResults",
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), "../artifacts/lambda/analyze_results")),
            memory_size=128,
            timeout=cdk.Duration.seconds(60)
        )
        data_bucket.grant_read(analyze_results_lambda)
        ssm.StringParameter(
            self,
            "AnalyzeResultsParameter",
            description="Analyze Results Lambda Function Name",
            parameter_name="AnalyzeResultsLambda",
            string_value=analyze_results_lambda.function_name
        )

        glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("glue.amazonaws.com")
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ]
        )
        data_bucket.grant_read_write(glue_role)

        glue_catalog = glue.Database(
            self,
            "GlueDatabase",
            database_name=f"{model_name}_new"
        )

        glue_crawler = glue.CfnCrawler(
            self,
            "GlueCrawler",
            name=f"{model_name}-crawler",
            role=glue_role.role_arn,
            database_name=glue_catalog.database_name,
            targets={
                "s3Targets": [
                    {
                        "path": f"s3://{data_bucket.bucket_name}/{model_name}_data/new/"
                    }
                ]
            }
        )
        ssm.StringParameter(
            self,
            "GlueCrawlerParameter",
            description="Glue Crawler Name",
            parameter_name="GlueCrawler",
            string_value=glue_crawler.name
        )

        glue_job = glue.CfnJob(
            self,
            "GlueETLJob",
            name=f"{model_name}-etl-job",
            description="AWS Glue ETL Job to merge new + raw data, and process training data",
            role=glue_role.role_arn,
            glue_version="2.0",
            execution_property=glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=1
            ),
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{data_bucket.bucket_name}/airflow/scripts/preprocess.py"
            ),
            default_arguments={
                "--job-language": "python",
                "--GLUE_CATALOG": glue_catalog.database_name,
                "--S3_BUCKET": data_bucket.bucket_name,
                "--S3_INPUT_KEY_PREFIX": f"{model_name}_data/raw/abalone.data",
                "--S3_OUTPUT_KEY_PREFIX": f"{model_name}_data",
                "--TempDir": f"s3://{data_bucket.bucket_name}/glue-temp"
            },
            allocated_capacity=5,
            timeout=10
        )
        ssm.StringParameter(
            self,
            "GlueJobParameter",
            description="Glue Job Name",
            parameter_name="GlueJob",
            string_value=glue_job.name
        )

        s3_deployment.BucketDeployment(
            self,
            "DeployData",
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), "../artifacts/data"))
            ],
            destination_bucket=data_bucket,
            destination_key_prefix=f"{model_name}_data/raw",
            retain_on_delete=False
        )

        code_deployment = codebuild.Project(
            self,
            "CodeDeploymentProject",
            project_name="CodeDeploymentProject",
            description="CodeBuild Project to Copy Airflow Artifacts to S3",
            source=codebuild.Source.code_commit(
                repository=code_repo
            ),
            environment=codebuild.BuildEnvironment(
                build_image=codebuild.LinuxBuildImage.STANDARD_5_0
            ),
            environment_variables={
                "DATA_BUCKET": codebuild.BuildEnvironmentVariable(
                    value=data_bucket.bucket_name
                )
            },
            build_spec=codebuild.BuildSpec.from_object(
                {
                    "version": "0.2",
                    "phases": {
                        "install": {
                            "runtime-versions": {
                                "python": 3.8
                            },
                            "commands": [
                                "printenv",
                                "echo 'Updating Build Environment'",
                                "python -m pip install --upgrade pip",
                                "python -m pip install --upgrade boto3 awscli"
                            ]
                        },
                        "build": {
                            "commands": [
                                "echo 'Deploying Airflow Artifacts to S3'",
                                "cd artifacts",
                                "aws s3 sync airflow s3://${DATA_BUCKET}/airflow"
                            ]
                        },
                        "post_build": {
                            "commands": [
                                "echo 'Airflow Artifacts Deployment Complete'"
                            ]
                        }
                    }
                }
            )
        )
        data_bucket.grant_read_write(code_deployment.role)

        code_repo.on_commit(
            "StartDeploymentProject",
            target=targets.CodeBuildProject(code_deployment)
        )