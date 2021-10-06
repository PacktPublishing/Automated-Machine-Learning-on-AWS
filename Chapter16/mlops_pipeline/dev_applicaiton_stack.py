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

class DevApplicationStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, id: str, *, group_name: str, model_name: str, **kwargs):
        super().__init__(scope, id, **kwargs)

        # Declare the Endpoint Name for the Dev stage
        endpoint_name = f'{model_name}-dev-endpoint'

        # Create a SageMaker Role for the SageMaker Endpoint
        sagemaker_dev_role = iam.Role(
            self,
            'SageMakerDevRole',
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
            'DevModel',
            execution_role_arn=sagemaker_dev_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                # model_package_name=latest_model_arn
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
            'DevEndpointConfig',
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    initial_instance_count=1,
                    initial_variant_weight=1.0,
                    instance_type='ml.t2.large',
                    model_name=model.attr_model_name,
                    variant_name='AllTraffic'
                )
            ]
        )

        # Create the SageMaker Endpoint
        endpoint = sagemaker.CfnEndpoint(
            self,
            'DevEndpoint',
            endpoint_config_name=endpoint_config.attr_endpoint_config_name,
            endpoint_name=endpoint_name
        )

        # Define the website static Bucket and have cdk generate the name
        static_bucket = s3.Bucket(
            self,
            'StaticS3Bucket',
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        # Define the Website logs Bucket
        logs_bucket = s3.Bucket(
            self,
            'WebsiteS3LogsBucket',
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

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
            # function_name='DevFormHandler',
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
                            default_ttl=cdk.Duration.seconds(0),
                            compress=True
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
            price_class=cloudfront.PriceClass.PRICE_CLASS_100,
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
