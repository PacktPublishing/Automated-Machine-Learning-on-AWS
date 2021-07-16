#!/usr/bin/env python3

from aws_cdk import core as cdk
from mlops_pipeline.mlops_pipeline_stack import PipelineStack

# Set the account to execute the MLOps Pipeline
PIPELINE_ACCOUNT = '500842391574'

# Set the name of the ML Model being operationalized
MODEL = 'abalone'

# Set the name of the Model Group, containing versioned production models
# See https://docs.aws.amazon.com/sagemaker/latest/dg/model-registry-model-group.html
MODEL_GROUP = f'{MODEL.capitalize()}PackageGroup'

# Setting the model quality threshold high to ensure the pipeline completes.
# See https://operational-machine-learning-pipeline.workshop.aws/assets/Model_Framing_Example.html
# for the recommended Root Mean Square Error value
QUALITY_THRESHOLD = 3.1

app = cdk.App()

PipelineStack(
    app,
    'mlops-pipeline',
    env={
        'account': PIPELINE_ACCOUNT,
        'region': 'us-east-2'
    },
    model_name=MODEL,
    group_name=MODEL_GROUP,
    threshold=QUALITY_THRESHOLD
)

app.synth()
