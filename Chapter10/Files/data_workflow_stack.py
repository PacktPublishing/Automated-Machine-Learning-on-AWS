import os
import aws_cdk.core as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_ssm as ssm
import aws_cdk.aws_s3_deployment as s3_deployment
import aws_cdk.aws_ec2 as ec2
import aws_cdk.aws_iam as iam
import aws_cdk.aws_mwaa as mwaa
import aws_cdk.aws_lambda as lambda_


class DataWorkflowStack(cdk.Stack):
    def __init__(self, scope: cdk.Construct, id: str, *, airflow_environment_name: str=None, data_bucket_name: str=None, pipeline_name: str=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        data_bucket = s3.Bucket.from_bucket_name(
            self,
            "Data-Bucket",
            bucket_name=data_bucket_name
        )

        data_bucket_param = ssm.StringParameter.from_string_parameter_name(
            self,
            "Data-Bucket-Parameter",
            string_parameter_name="DataBucket"
        )

        group_name_param = ssm.StringParameter.from_string_parameter_name(
            self,
            "Feature-Group-Parameter",
            string_parameter_name="FeatureGroup"
        )

        vpc = ec2.Vpc(
            self,
            "Airflow-VPC",
            cidr="10.0.0.0/16",
            max_azs=2,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="AirflowPublicSubnet",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="AirflowPrivateSubnet",
                    subnet_type=ec2.SubnetType.PRIVATE,
                    cidr_mask=24
                )
            ],
            nat_gateways=2,
            enable_dns_hostnames=True,
            enable_dns_support=True
        )

        airflow_sg = ec2.SecurityGroup(
            self,
            "Airflow-SG",
            vpc=vpc,
            description="Airflow Internal Traffic",
            security_group_name=f"{airflow_environment_name}-sg"
        )
        airflow_sg.connections.allow_internally(ec2.Port.all_traffic(), "MWAA")

        airflow_subnet_ids = list(map(lambda x: x.subnet_id, vpc.private_subnets))

        airflow_network = mwaa.CfnEnvironment.NetworkConfigurationProperty(
            security_group_ids=[
                airflow_sg.security_group_id
            ],
            subnet_ids=airflow_subnet_ids
        )

        airflow_policy_document = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": "airflow:PublishMetrics",
                    "Resource": f"arn:aws:airflow:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:environment/{airflow_environment_name}"
                },
                {
                    "Effect": "Deny",
                    "Action": "s3:ListAllMyBuckets",
                    "Resource": [
                        f"arn:aws:s3:::{data_bucket.bucket_name}",
                        f"arn:aws:s3:::{data_bucket.bucket_name}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject*",
                        "s3:GetBucket*",
                        "s3:List*"
                    ],
                    "Resource": [
                        f"arn:aws:s3:::{data_bucket.bucket_name}",
                        f"arn:aws:s3:::{data_bucket.bucket_name}/*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:CreateLogStream",
                        "logs:CreateLogGroup",
                        "logs:PutLogEvents",
                        "logs:GetLogEvents",
                        "logs:GetLogRecord",
                        "logs:GetLogGroupFields",
                        "logs:GetQueryResults"
                    ],
                    "Resource": [
                        f"arn:aws:logs:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:log-group:airflow-{airflow_environment_name}-*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "logs:DescribeLogGroups"
                    ],
                    "Resource": [
                        "*"
                    ]
                },
                {
                    "Effect": "Allow",
                    "Action": "cloudwatch:PutMetricData",
                    "Resource": "*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "sqs:ChangeMessageVisibility",
                        "sqs:DeleteMessage",
                        "sqs:GetQueueAttributes",
                        "sqs:GetQueueUrl",
                        "sqs:ReceiveMessage",
                        "sqs:SendMessage"
                    ],
                    "Resource": f"arn:aws:sqs:{cdk.Aws.REGION}:*:airflow-celery-*"
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:GenerateDataKey*",
                        "kms:Encrypt"
                    ],
                    "NotResource": f"arn:aws:kms:*:{cdk.Aws.ACCOUNT_ID}:key/*",
                    "Condition": {
                        "StringLike": {
                            "kms:ViaService": [
                                f"sqs.{cdk.Aws.REGION}.amazonaws.com"
                            ]
                        }
                    }
                }
            ]
        }

        airflow_role = iam.Role(
            self,
            "AirflowRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("airflow.amazonaws.com"),
                iam.ServicePrincipal("airflow-env.amazonaws.com")
            ),
            inline_policies=[
                iam.PolicyDocument.from_json(airflow_policy_document)
            ],
            path="/service-role/"
        )
        airflow_role.add_managed_policy(policy=iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"))
        airflow_role.add_managed_policy(policy=iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFeatureStoreAccess"))
        data_bucket.grant_read_write(airflow_role)
        data_bucket_param.grant_read(airflow_role)
        group_name_param.grant_read(airflow_role)
        
        airflow_emvironment = mwaa.CfnEnvironment(
            self,
            "Airflow-Environment",
            name=airflow_environment_name,
            airflow_version="2.0.2",
            airflow_configuration_options={
                "core.default_timezone": "utc",
                "logging.logging_level": "INFO"
            },
            execution_role_arn=airflow_role.role_arn,
            environment_class="mw1.small",
            max_workers=5,
            source_bucket_arn=data_bucket.bucket_arn,
            dag_s3_path="airflow/dags",
            requirements_s3_path="airflow/requirements.txt",
            logging_configuration=mwaa.CfnEnvironment.LoggingConfigurationProperty(
                dag_processing_logs=mwaa.CfnEnvironment.ModuleLoggingConfigurationProperty(
                    enabled=True,
                    log_level="INFO"
                ),
            ),
            network_configuration=airflow_network,
            webserver_access_mode="PUBLIC_ONLY"
        )

        artifacts_deployment = s3_deployment.BucketDeployment(
            self,
            "Deploy-Airflow-Artifacts",
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), "../airflow"))
            ],
            destination_bucket=data_bucket,
            destination_key_prefix="airflow",
            retain_on_delete=False
        )
        airflow_emvironment.node.add_dependency(artifacts_deployment)

        start_pipeline = lambda_.Function(
            self,
            "Release-Change",
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), "../lambda/releaseChange")),
            environment={
                "PIPELINE_NAME": pipeline_name
            },
            memory_size=128,
            timeout=cdk.Duration.seconds(60)
        )
        start_pipeline.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "codepipeline:StartPipelineExecution"
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    f"arn:aws:codepipeline:{cdk.Aws.REGION}:{cdk.Aws.ACCOUNT_ID}:{pipeline_name}"
                ]
            )
        )
        start_pipeline.grant_invoke(airflow_role)
