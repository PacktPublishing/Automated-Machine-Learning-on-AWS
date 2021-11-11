import os
from time import strftime
import aws_cdk.core as cdk
import aws_cdk.aws_s3 as s3
import aws_cdk.aws_cloudfront as cloudfront
import aws_cdk.aws_iam as iam
import aws_cdk.aws_s3_deployment as s3_deployment
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_apigatewayv2 as httpgw
import aws_cdk.aws_apigatewayv2_integrations as integrations
import aws_cdk.aws_sagemaker as sagemaker
import aws_cdk.custom_resources as cr

class TestApplicaitonStack(cdk.Stack):

    def __init__(self, scope: cdk.Construct, id: str, *, model_name: str=None, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        endpoint_name = f"{model_name}-test-endpoint"

        sagemaker_test_role = iam.Role(
            self,
            "SageMaker-TestRole",
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
            "Test-Model",
            execution_role_arn=sagemaker_test_role.role_arn,
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
            "Test-EndpointConfig",
            production_variants=[
                sagemaker.CfnEndpointConfig.ProductionVariantProperty(
                    initial_instance_count=1,
                    initial_variant_weight=1.0,
                    instance_type="ml.t2.large",
                    model_name=model.attr_model_name,
                    variant_name="AllTraffic"
                )
            ]
        )

        endpoint = sagemaker.CfnEndpoint(
            self,
            "Test-Endpoint",
            endpoint_config_name=endpoint_config.attr_endpoint_config_name,
            endpoint_name=endpoint_name
        )
        endpoint.add_depends_on(endpoint_config)

        static_bucket = s3.Bucket(
            self,
            "Static-Bucket",
            removal_policy=cdk.RemovalPolicy.DESTROY
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
                os.path.join(os.path.dirname(__file__),
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
            integration=integrations.LambdaProxyIntegration(handler=form_lambda)
        )
        api.add_routes(
            path="/api/predict",
            methods=[httpgw.HttpMethod.POST],
            integration=integrations.LambdaProxyIntegration(handler=form_lambda)
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
                            default_ttl=cdk.Duration.seconds(0),
                            compress=True
                        )
                    ]
                )
            ],
            default_root_object="index.html",
            enable_ip_v6=True,
            http_version=cloudfront.HttpVersion.HTTP2,
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

        self.cdn_output = cdk.CfnOutput(
            self,
            "CloudFront-URL",
            value=f"http://{cdn.domain_name}"
        )

        self.api_output = cdk.CfnOutput(
            self,
            "Form-API-URL",
            value=api.url
        )
