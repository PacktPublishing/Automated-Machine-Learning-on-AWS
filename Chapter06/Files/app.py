#!/usr/bin/env python3

import os
from aws_cdk import core as cdk
from abalone_cicd_pipeline.abalone_endpoint_stack import EndpointStack
from abalone_cicd_pipeline.abalone_cicd_pipeline_stack import PipelineStack


MODEL = "abalone"
CODECOMMIT_REPOSITORY = "abalone-cicd-pipeline"
CDK_VERSION = "<ENTER THE CDK VERSION>"

app = cdk.App()

EndpointStack(
    app,
    "EndpointStack",
    env=cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")),
    model_name=MODEL,
    repo_name=CODECOMMIT_REPOSITORY
)

PipelineStack(
    app,
    CODECOMMIT_REPOSITORY,
    env=cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")),
    model_name=MODEL,
    repo_name=CODECOMMIT_REPOSITORY,
    cdk_version=CDK_VERSION
)

app.synth()