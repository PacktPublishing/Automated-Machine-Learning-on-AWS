#!/usr/bin/env python3
import os
from aws_cdk import core as cdk
from abalone_data_pipeline.abalone_data_pipeline_stack import DataPipelineStack

MODEL = "abalone"
CODECOMMIT_REPOSITORY = "abalone-data-pipeline"

app = cdk.App()

DataPipelineStack(
    app,
    CODECOMMIT_REPOSITORY,
    env=cdk.Environment(account=os.getenv("CDK_DEFAULT_ACCOUNT"), region=os.getenv("CDK_DEFAULT_REGION")),
    model_name=MODEL,
    repo_name=CODECOMMIT_REPOSITORY,
    airflow_environment_name=f"{MODEL}-airflow-environment"
)

app.synth()