import os
from time import strftime
import aws_cdk as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_cloudfront as cloudfront
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3_deployment as s3_deployment
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_apigatewayv2_alpha as httpgw
import aws_cdk.aws_apigatewayv2_integrations_alpha as integrations
import aws_cdk.aws_sagemaker as sagemaker
import aws_cdk.custom_resources as cr
import aws_cdk.aws_applicationautoscaling as autoscaling
from constructs import Construct

class ProductionApplicaitonStack(cdk.Stack):

    def __init__(self, scope: Construct, id: str, *, model_name: str=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        endpoint_name = f"{model_name}-prod-endpoint"

        static_bucket = s3.Bucket(
            self,
            "Static-Bucket",
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        logs_bucket = s3.Bucket(
            self,
            "Logs-Bucket",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            versioned=True
        )

        baseline_source_uri = cr.AwsCustomResource(
            self,
            "Baseline-Uri-Parameter",
            on_create=cr.AwsSdkCall(
                action="getParameter",
                service="SSM",
                parameters={
                    "Name": "BaselineDataUri"
                },
                physical_resource_id=cr.PhysicalResourceId.of(strftime("%Y%m%d%H%M%S"))
            ),
            on_update=cr.AwsSdkCall(
                action="getParameter",
                service="SSM",
                parameters={
                    "Name": "BaselineDataUri"
                },
                physical_resource_id=cr.PhysicalResourceId.of(strftime("%Y%m%d%H%M%S"))
            ),
            policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
            )
        ).get_response_field("Parameter.Value")

        sagemaker_prod_role = iam.Role(
            self,
            "SageMaker-ProdRole",
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("sagemaker.amazonaws.com")
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSageMakerFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess")
            ]
        )

        model = sagemaker.CfnModel(
            self,
            "Prod-Model",
            execution_role_arn=sagemaker_prod_role.role_arn,
            primary_container=sagemaker.CfnModel.ContainerDefinitionProperty(
                model_package_name=cr.AwsCustomResource(
                    self,
                    "Get-ModelPackage-Parameter",
                    on_create=cr.AwsSdkCall(
                        action="getParameter",
                        service="SSM",
                        parameters={
                            "Name": "ModelPackageName"
                        },
                        physical_resource_id=cr.PhysicalResourceId.of(strftime("%Y%m%d%H%M%S"))
                    ),
                    on_update=cr.AwsSdkCall(
                        action="getParameter",
                        service="SSM",
                        parameters={
                            "Name": "ModelPackageName"
                        },
                        physical_resource_id=cr.PhysicalResourceId.of(strftime("%Y%m%d%H%M%S"))
                    ),
                    policy=cr.AwsCustomResourcePolicy.from_sdk_calls(
                        resources=cr.AwsCustomResourcePolicy.ANY_RESOURCE
                    )
                ).get_response_field("Parameter.Value")
            )
        )

        endpoint_config = sagemaker.CfnEndpointConfig(
            self,
            "Prod-EndpointConfig",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    initial_instance_count=2,
                    initial_variant_weight=1.0,
                    instance_type="ml.c5.large",
                    model_name=model.attr_model_name,
                    variant_name="AllTraffic"
                )
            ],
            data_capture_config=sagemaker.CfnEndpointConfig.DataCaptureConfigProperty(
                enable_capture=True,
                capture_content_type_header=sagemaker.CfnEndpointConfig.CaptureContentTypeHeaderProperty(
                    csv_content_types=["text/csv"]
                ),
                capture_options=[
                    sagemaker.CfnEndpointConfig.CaptureOptionProperty(capture_mode="Input"),
                    sagemaker.CfnEndpointConfig.CaptureOptionProperty(capture_mode="Output")
                ],
                destination_s3_uri=f"s3://{logs_bucket.bucket_name}/endpoint-data-capture",
                initial_sampling_percentage=100.0
            )
        )

        endpoint = sagemaker.CfnEndpoint(
            self,
            "Prod-Endpoint",
            endpoint_config_name=endpoint_config.attr_endpoint_config_name,
            endpoint_name=endpoint_name
        )
        endpoint.add_depends_on(endpoint_config)

        baseline_creator = lambda_.Function(
            self,
            "Suggest-Baseline",
            handler="index.lambda_handler",
            runtime=lambda_.Runtime.PYTHON_3_8,
            code=lambda_.Code.from_asset(
                os.path.join(
                    os.path.dirname(__file__),
                    "../../lambda/createBaseline"
                )
            ),
            memory_size=128,
            timeout=cdk.Duration.seconds(120)
        )
        baseline_creator.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "sagemaker:*ProcessingJob",
                    "s3:*",
                    "iam:PassRole"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )
        cdk.CustomResource(
            self,
            "Invoke-Baseline-Creator",
            service_token=cr.Provider(
                self,
                "Suhhest-Baseline-Provider",
                on_event_handler=baseline_creator
            ).service_token,
            properties={
                "BaselineSourceUri": baseline_source_uri,
                "LogsBucketName": logs_bucket.bucket_name,
                "RoleArn": sagemaker_prod_role.role_arn
            }
        )

        origin = cloudfront.OriginAccessIdentity(
            self,
            "Bucket-Origin",
            comment="Origin associated with ACME website static content Bucket"
        )

        static_bucket.grant_read(
            iam.CanonicalUserPrincipal(
                origin.cloud_front_origin_access_identity_s3_canonical_user_id
            )
        )

        form_lambda = lambda_.DockerImageFunction(
            self,
            "Form-Lambda",
            code=lambda_.DockerImageCode.from_image_asset(
                os.path.join(
                    os.path.dirname(__file__),
                    "../../lambda/formHandler"
                )
            ),
            environment={
                "sagemakerEndpoint": endpoint.attr_endpoint_name
            },
            memory_size=512,
            timeout=cdk.Duration.seconds(120)
        )
        form_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "sagemaker:InvokeEndpoint"
                ],
                effect=iam.Effect.ALLOW,
                resources=["*"]
            )
        )

        api = httpgw.HttpApi(
            self,
            "Form-API",
            cors_preflight={
                "allow_origins": ["*"],
                "allow_methods": [httpgw.HttpMethod.POST],
                "allow_headers": ["*"]
            }
        )
        api.add_routes(
            path="/api/contact",
            methods=[httpgw.HttpMethod.POST],
            integration=integrations.HttpLambdaIntegration(
                "ContactForm-Integration",
                handler=form_lambda
            )
        )
        api.add_routes(
            path="/api/predict",
            methods=[httpgw.HttpMethod.POST],
            integration=integrations.HttpLambdaIntegration(
                "PredictForm-Integration",
                handler=form_lambda
            )
        )

        cdn = cloudfront.CloudFrontWebDistribution(
            self,
            "CloudFront-CDN",
            comment="CDN for the ACME website",
            origin_configs=[
                cloudfront.SourceConfiguration(
                    custom_origin_source=cloudfront.CustomOriginConfig(
                        domain_name=f"{api.http_api_id}.execute-api.{cdk.Aws.REGION}.amazonaws.com"
                    ),
                    behaviors=[
                        cloudfront.Behavior(
                            allowed_methods=cloudfront.CloudFrontAllowedMethods.ALL,
                            default_ttl=cdk.Duration.seconds(0),
                            forwarded_values={
                                "query_string": True,
                                "headers": ["Authorization"]
                            },
                            path_pattern="/api/*"
                        )
                    ]
                ),
                cloudfront.SourceConfiguration(
                    s3_origin_source=cloudfront.S3OriginConfig(
                        s3_bucket_source=static_bucket,
                        origin_access_identity=origin
                    ),
                    behaviors=[
                        cloudfront.Behavior(
                            is_default_behavior=True,
                            min_ttl=cdk.Duration.minutes(10),
                            max_ttl=cdk.Duration.minutes(20),
                            default_ttl=cdk.Duration.minutes(10),
                            compress=True
                        )
                    ]
                )
            ],
            default_root_object="index.html",
            enable_ip_v6=True,
            http_version=cloudfront.HttpVersion.HTTP2,
            logging_config=cloudfront.LoggingConfiguration(
                bucket=logs_bucket,
                include_cookies=True
            ),
            price_class=cloudfront.PriceClass.PRICE_CLASS_100,
            viewer_protocol_policy=cloudfront.ViewerProtocolPolicy.REDIRECT_TO_HTTPS
        )

        s3_deployment.BucketDeployment(
            self,
            "Deploy-Website",
            sources=[
                s3_deployment.Source.asset(os.path.join(os.path.dirname(__file__), "../../www"))
            ],
            destination_bucket=static_bucket,
            distribution=cdn,
            retain_on_delete=False
        )

        scaling_target = autoscaling.CfnScalableTarget(
            self,
            "Autoscaling-Target",
            min_capacity=2,
            max_capacity=10,
            resource_id=f"endpoint/{endpoint_name}/variant/AllTraffic",
            role_arn=sagemaker_prod_role.role_arn,
            scalable_dimension="sagemaker:variant:DesiredInstanceCount",
            service_namespace="sagemaker"
        )
        scaling_target.add_depends_on(endpoint)

        scaling_policy = autoscaling.CfnScalingPolicy(
            self,
            "Autoscaling-Policy",
            policy_name="SageMakerVariantInvoicationsPerInstace",
            policy_type="TargetTrackingScaling",
            resource_id=f"endpoint/{endpoint_name}/variant/AllTraffic",
            scalable_dimension="sagemaker:variant:DesiredInstanceCount",
            service_namespace="sagemaker",
            target_tracking_scaling_policy_configuration=autoscaling.CfnScalingPolicy.TargetTrackingScalingPolicyConfigurationProperty(
                target_value=750.0,
                scale_in_cooldown=60,
                scale_out_cooldown=60,
                predefined_metric_specification=autoscaling.CfnScalingPolicy.PredefinedMetricSpecificationProperty(
                    predefined_metric_type="SageMakerVariantInvocationsPerInstance"
                )
            )
        )
        scaling_policy.add_depends_on(scaling_target)

        self.cdn_output = cdk.CfnOutput(
            self,
            "CloudFront-URL",
            value=f"http://{cdn.distribution_domain_name}"
        )

        self.api_output = cdk.CfnOutput(
            self,
            "Form-API-URL",
            value=api.url
        )
