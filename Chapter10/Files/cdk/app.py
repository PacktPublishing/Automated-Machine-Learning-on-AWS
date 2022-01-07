#!/usr/bin/env python3
import os
import aws_cdk as cdk
from acme_web_application.acme_pipeline_stack import PipelineStack

MODEL = "abalone"
MODEL_GROUP = f"{MODEL.capitalize()}PackageGroup"
FEATURE_GROUP = "PLACEHOLDER"
CODECOMMIT_REPOSITORY = "acme-web-application"
CDK_VERSION = "<Add the version of the AWS CDK you are currently using>"
QUALITY_THRESHOLD = 3.1

app = cdk.App()

PipelineStack(
    app,
    CODECOMMIT_REPOSITORY,
    env=cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")),
    model_name=MODEL,
    repo_name=CODECOMMIT_REPOSITORY,
    group_name=MODEL_GROUP,
    feature_group=FEATURE_GROUP,
    cdk_version=CDK_VERSION,
    threshold=QUALITY_THRESHOLD,
)

app.synth()
