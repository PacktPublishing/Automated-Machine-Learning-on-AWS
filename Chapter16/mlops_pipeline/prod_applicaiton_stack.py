import os
from time import strftime
import aws_cdk.core as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_cloudfront as cloudfront
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3_deployment as s3_deployment
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_apigateway as apigw
import aws_cdk.aws_codedeploy as codedeploy
import aws_cdk.aws_cloudwatch as cloudwatch
import aws_cdk.aws_codebuild as codebuild
import aws_cdk.aws_apigatewayv2 as httpgw
import aws_cdk.aws_apigatewayv2_integrations as integrations
import aws_cdk.aws_sagemaker as sagemaker
import aws_cdk.custom_resources as cr
import aws_cdk.aws_applicationautoscaling as autoscaling

class ProdApplicationStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, id: str, *, group_name: str, model_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Declare the Endpoint Name for the Dev stage
        endpoint_name = f'{model_name}-prod-endpoint'

        """
        # Get the Data Bucket Name from the parameter store using a Custom Resource
        # NOTE: There is no `region` parameter specifed on the `AwsSdkCall` as this is an anti-pattern. 
        #       Therefore, consider deploying the 'Prod' stack in the same region as the pipeline.
        data_bucket = cr.AwsCustomResource(
            self,
            'GetBucketNameParameter',
            on_create=cr.AwsSdkCall(
                action='getParameter',
                service='SSM',
                parameters={
                    'Name': 'DataBucketName'
                },
                physical_resource_id=cr.PhysicalResourceId.of(strftime('%Y%m%d%H%M%S'))
            ),
            on_update=cr.AwsSdkCall(
                action='getParameter',
                service='SSM',
                parameters={
                    'Name': 'DataBucketName'
                },
                physical_resource_id=cr.PhysicalResourceId.of(strftime('%Y%m%d%H%M%S'))
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            ),
        ).get_response_field('Parameter.Value')
        """

        # Get the location of the testing data from  the parameter store using a Custom Resource
        # NOTE: There is no `region` parameter specifed on the `AwsSdkCall` as this is an anti-pattern. 
        #       Therefore, consider deploying the 'Prod' stack in the same region as the pipeline.
        baseline_source_uri = cr.AwsCustomResource(
            self,
            'BaselineUriParameter',
            on_create=cr.AwsSdkCall(
                action='getParameter',
                service='SSM',
                parameters={
                    'Name': 'BaselineDataUri'
                },
                physical_resource_id=cr.PhysicalResourceId.of(strftime('%Y%m%d%H%M%S'))
            ),
            on_update=cr.AwsSdkCall(
                action='getParameter',
                service='SSM',
                parameters={
                    'Name': 'BaselineDataUri'
                },
                physical_resource_id=cr.PhysicalResourceId.of(strftime('%Y%m%d%H%M%S'))
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            )
        ).get_response_field('Parameter.Value')

        # Define the website static Bucket and have cdk generate the name
        static_bucket = s3.Bucket(
            self,
            'StaticS3Bucket',
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        # Define the Website and Endpoint logs Bucket
        logs_bucket = s3.Bucket(
            self,
            'LogsS3Bucket',
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True
        )

        # Create a SageMaker Role for the SageMaker Endpoint
        sagemaker_prod_role = iam.Role(
            self,
            'SageMakerProdRole',
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal('sagemaker.amazonaws.com')
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonSageMakerFullAccess'),
                iam.ManagedPolicy.from_aws_managed_policy_name('AmazonS3FullAccess')
            ]
        )

        # Create a SageMaker Model using the latest trained model artifacts from the model registry
        model = sagemaker.CfnModel(
            self,
            'ProdModel',
            execution_role_arn=sagemaker_prod_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                model_package_name=cr.AwsCustomResource(
                    self,
                    'GetModelPackageParameter',
                    on_create=cr.AwsSdkCall(
                        action='getParameter',
                        service='SSM',
                        parameters={
                            'Name': 'ModelPackageName'
                        },
                        physical_resource_id=cr.PhysicalResourceId.of(strftime('%Y%m%d%H%M%S'))
                    ),
                    on_update=cr.AwsSdkCall(
                        action='getParameter',
                        service='SSM',
                        parameters={
                            'Name': 'ModelPackageName'
                        },
                        physical_resource_id=cr.PhysicalResourceId.of(strftime('%Y%m%d%H%M%S'))
                    ),
                    policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                        resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
                    )
                ).get_response_field('Parameter.Value')
            )
        )

        # Create the SageMaker Endpoint Config for the model
        endpoint_config = sagemaker.CfnEndpointConfig(
            self,
            'ProdEndpointConfig',
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    initial_instance_count=2,
                    initial_variant_weight=1.0,
                    instance_type='ml.c5.large',
                    model_name=model.attr_model_name,
                    variant_name='AllTraffic'
                )
            ],
            data_capture_config=sagemaker.CfnEndpointConfig.DataCaptureConfigProperty(
                capture_content_type_header=sagemaker.CfnEndpointConfig.CaptureContentTypeHeaderProperty(
                    csv_content_types=[
                        'text/csv'
                    ]
                ),
                capture_options=[
                    sagemaker.CfnEndpointConfig.CaptureOptionProperty(capture_mode='Input'),
                    sagemaker.CfnEndpointConfig.CaptureOptionProperty(capture_mode='Output')
                ],
                destination_s3_uri=f's3://{logs_bucket.bucket_name}/endpoint-data-capture',
                enable_capture=True,
                initial_sampling_percentage=100.0
            )
        )

        # Create the SageMaker Endpoint
        endpoint = sagemaker.CfnEndpoint(
            self,
            'ProdEndpoint',
            endpoint_config_name=endpoint_config.attr_endpoint_config_name,
            endpoint_name=endpoint_name
        )
        endpoint.add_depends_on(endpoint_config)

        # Create a Model Quality Baseline using a Custom Resource
        # NOTE:  A Custom Resource is used due to the fact that the 'baseline dataset' is part of the ML Workflow
        #        Stack (crteated as part of the 'Evaluation Step'), which could be in another AWS Region or AWS 
        #        Account.
        baseline_creator = lambda_.Function(
            self,
            'SuggestBaseline',
            handler='index.lambda_handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), '..', 'lambda', 'createBaseline')),
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )

        # Add the necessary permissions to the 'baseline creator' function
        baseline_creator.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    'sagemaker:*ProcessingJob*',
                    's3:*',
                    'iam:PassRole'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*']
            )
        )

        # Invoke the custom resource to suggest the modsel quality baseline
        # NOTE: A custom resource Lambda Function is being used here to ensure that the baseline data moves to the 
        #       Logs Bucket before creating the Baseline Processing as well as allowing customization of the 
        #       'sagemaker-model-monitor-analyzer' to the specific AWS Region
        suggest_baseline = cdk.CustomResource(
            self,
            'InvokeSuggestBaseline',
            service_token=cr.Provider(
                self,
                'SuggestBaselineProvider',
                on_event_handler=baseline_creator
            ).service_token,
            properties={
                'BaselineSourceUri': baseline_source_uri,
                'LogsBucketName': logs_bucket.bucket_name,
                'RoleArn': sagemaker_prod_role.role_arn
            }
        )

        """
        # Create a SageMaker Monitoring Schedule to compare the training baseline with data captured from new inferences
        # NOTE: A Custom Resource is used to create the scheduling job in lieu of using `CfnMonitoringSchedule`.
        #       See https://github.com/aws/aws-cdk/issues/12208
        monitoring_schedule = cr.AwsCustomResource(
            self,
            'MonitoringSchedule',
            on_create=cr.AwsSdkCall(
                action='createMonitoringSchedule',
                service='SageMaker',
                parameters={
                    "MonitoringScheduleConfig": {
                        "MonitoringJobDefinition": { 
                            "BaselineConfig": {
                                "ConstraintsResource": {
                                    "S3Uri": f"s3://{logs_bucket}/baseline_report/constraints.json"
                                },
                                "StatisticsResource": {
                                    "S3Uri": f"s3://{logs_bucket}/baseline_report/statistics.json"
                                }
                            },
                            "MonitoringAppSpecification": {
                                "ImageUri": f"159807026194.dkr.ecr.{cdk.Aws.REGION}.amazonaws.com/sagemaker-model-monitor-analyzer:latest"
                            },
                            "MonitoringInputs": [
                                {
                                    "EndpointInput": { 
                                        "EndpointName": endpoint.attr_endpoint_name,
                                        "LocalPath": "/opt/ml/processing/endpointdata",
                                    }
                                }
                            ],
                            "MonitoringOutputConfig": {
                                "MonitoringOutputs": [ 
                                    {
                                        "S3Output": { 
                                            "LocalPath": "/opt/ml/processing/localpath",
                                            "S3Uri": f"s3://{logs_bucket}/reports"
                                        }
                                    }
                                ]
                            },
                            "MonitoringResources": { 
                                "ClusterConfig": { 
                                    "InstanceCount": 1,
                                    "InstanceType": "ml.m5.large",
                                    "VolumeSizeInGB": 50
                                }
                            },
                            "RoleArn": sagemaker_prod_role.role_arn,
                        },
                        "ScheduleConfig": { 
                            "ScheduleExpression": "cron(0 * ? * * *)"
                        }
                    },
                    "MonitoringScheduleName": f"{model_name}MonitoringSchedule"
                },
                physical_resource_id=cr.PhysicalResourceId.of(f"{model_name}MonitoringSchedule")
            ),
            on_delete=cr.AwsSdkCall(
                action='deleteMonitoringSchedule',
                service='SageMaker',
                parameters={
                    "MonitoringScheduleName": f"{model_name}MonitoringSchedule"
                },
                physical_resource_id=cr.PhysicalResourceId.of(f"{model_name}MonitoringSchedule")
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            )
        )
        sagemaker_prod_role.grant_pass_role(monitoring_schedule)
        """

        # Enable AutoScaling for the SageMaker Endpoint
        scaling_target = autoscaling.CfnScalableTarget(
            self,
            'AutoScalingTarget',
            max_capacity=10,
            min_capacity=2,
            resource_id=f'endpoint/{endpoint_name}/variant/AllTraffic',
            role_arn=sagemaker_prod_role.role_arn,
            scalable_dimension='sagemaker:variant:DesiredInstanceCount',
            service_namespace='sagemaker'
        )
        scaling_target.add_depends_on(endpoint)

        # Create a Scaling Policy to govern the SageMaker Endpoint Autoscaling
        scaling_policy = autoscaling.CfnScalingPolicy(
            self,
            'AutoScalingPolicy',
            policy_name='SageMakerVariantInvocationsPerInstance',
            policy_type='TargetTrackingScaling',
            resource_id=f'endpoint/{endpoint_name}/variant/AllTraffic',
            scalable_dimension='sagemaker:variant:DesiredInstanceCount',
            service_namespace='sagemaker',
            target_tracking_scaling_policy_configuration=autoscaling.CfnScalingPolicy.TargetTrackingScalingPolicyConfigurationProperty(
                target_value=750.0,
                scale_in_cooldown=60,
                scale_out_cooldown=60,
                predefined_metric_specification=autoscaling.CfnScalingPolicy.PredefinedMetricSpecificationProperty(
                    predefined_metric_type='SageMakerVariantInvocationsPerInstance'
                )
            )
        )
        scaling_policy.add_depends_on(scaling_target)

        # Obtain the cloudfront origin access identity so that the s3 bucket may be restricted to it.
        origin = cloudfront.OriginAccessIdentity(
            self,
            'BucketOrigin',
            comment='Origin associated with abalone website static S3 bucket',
        )

        # Restrict the S3 bucket via a bucket policy that only allows our CloudFront distribution.
        static_bucket.grant_read(
            iam.CanonicalUserPrincipal(origin.cloud_front_origin_access_identity_s3_canonical_user_id)
        )

        # Create the new Lambda based on the `container`
        # form_lambda = lambda_.Function(
        #     self,
        #     'FormLambda',
        #     runtime=lambda_.Runtime.PYTHON_3_8,
        #     code = lambda_.Code.from_asset(os.path.join(os.path.dirname(__file__), '..', 'lambda', 'formHandler')),
        #     handler='index.lambda_handler',
        #     environment={
        #         'sagemakerEndpoint': endpoint.attr_endpoint_name
        #     },
        #     memory_size=128,
        #     timeout=cdk.Duration.seconds(120)
        # )
        form_lambda = lambda_.DockerImageFunction(
            self,
            'FormLambda',
            # function_name='ProdFormHandler',
            code=lambda_.DockerImageCode.from_image_asset(os.path.join(os.path.dirname(__file__), '..', 'lambda', 'formHandler')),
            environment={
                'sagemakerEndpoint': endpoint.attr_endpoint_name
            },
            memory_size=512,
            timeout=cdk.Duration.seconds(120)
        )

        # Give the 'form'handler' function access to call the SageMaker endpoint
        form_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    'sagemaker:InvokeEndpoint'
                ],
                effect=iam.Effect.ALLOW,
                resources=['*']
            )
        )

        # Create the HTTP API Gateway
        api = httpgw.HttpApi(
            self,
            'FormAPI',
            cors_preflight={
                'allow_origins': ['*'],
                'allow_methods': [httpgw.HttpMethod.POST],
                'allow_headers': ['*']
            }
        )

        # Create the Route Integration between the HTTP API and the Form Lambda
        default_integration = integrations.LambdaProxyIntegration(handler=form_lambda)

        # Create the API routes for the Contact Form
        api.add_routes(
            path='/api/contact',
            methods=[httpgw.HttpMethod.POST],
            integration=default_integration
        )

        # Creat the API route for the Prediction Form
        api.add_routes(
            path='/api/predict',
            methods=[httpgw.HttpMethod.POST],
            integration=default_integration
        )
        
        # Define the CloudFront Distibution
        cdn = cloudfront.CloudFrontWebDistribution(
            self,
            'CloudFrontCDN',
            comment='CDN for the abalone website',
            origin_configs=[
                cloudfront.SourceConfiguration(
                    custom_origin_source=cloudfront.CustomOriginConfig(
                        domain_name=f'{api.http_api_id}.execute-api.{self.region}.amazonaws.com'
                    ),
                    behaviors=[
                        cloudfront.Behavior(
                            allowed_methods=cloudfront.CloudFrontAllowedMethods.ALL,
                            default_ttl=cdk.Duration.seconds(0),
                            forwarded_values={
                                'query_string': True,
                                'headers': ['Authorization']
                            },
                            path_pattern='/api/*'
                        )
                    ]
                ),
                cloudfront.SourceConfiguration(
                    s3_origin_source=cloudfront.S3OriginConfig(
                        s3_bucket_source=static_bucket,
                        origin_access_identity=origin,
                    ),
                    behaviors=[
                        cloudfront.Behavior(
                            is_default_behavior=True,
                            min_ttl=cdk.Duration.minutes(10),
                            max_ttl=cdk.Duration.minutes(20),
                            default_ttl=cdk.Duration.minutes(10),
                            compress=True,
                        )
                    ],
                )
            ],
            default_root_object='index.html',
            enable_ip_v6=True,
            http_version=cloudfront.HttpVersion.HTTP2,
            logging_config=cloudfront.LoggingConfiguration(
                bucket=logs_bucket,
                include_cookies=True
            ),
            price_class=cloudfront.PriceClass.PRICE_CLASS_ALL,
            viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS
        )
        
        # CDK helper that takes the defined source directory, compresses it, and uploads it to the destination s3 bucket.
        s3_deployment.BucketDeployment(
            self,
            'DeployWebsite',
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), '..', 'www'))
            ],
            destination_bucket=static_bucket,
            distribution=cdn,
            retain_on_delete=False
        )

        # Create a CDK Output which details the URL for the CloudFront Distribtion URL.
        self.cdn_output = cdk.CfnOutput(
            self,
            'CloudFront distribution URL',
            value=f"http://{cdn.domain_name}"
        )

        # Create the CDK Output for the HTTP APIGateway
        self.httpapi_output = cdk.CfnOutput(
            self,
            'Form Processing API',
            value=api.url
        )
