#!/usr/bin/env python3

# Basic Python script to clean up experiments
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

config = Config(retries = {'max_attempts': 10, 'mode': 'adaptive'})
sm = boto3.client('sagemaker', config=config)
model_name = 'abalone'

def get_name(name):
    response = sm.list_feature_groups(
        NameContains=name
    )
    if response['FeatureGroupSummaries'] == []:
        return None
    else:
        return response['FeatureGroupSummaries'][0]['FeatureGroupName']

response = sm.delete_feature_group(
    FeatureGroupName=get_name('abalone')
)
print(response)