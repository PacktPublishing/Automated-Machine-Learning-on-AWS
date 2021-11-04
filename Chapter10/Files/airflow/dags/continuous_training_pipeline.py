import time
import json
import sagemaker
import boto3
import numpy as np
import pandas as pd
from time import gmtime, strftime
from datetime import timedelta
from sagemaker.feature_store.feature_group import FeatureGroup

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor

region_name = "<Add AWS Region>"
data_bucket = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="DataBucket")["Parameter"]["Value"]}"""
lambda_function = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="ReleaseChangeLambda")["Parameter"]["Value"]}"""
fg_name = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="FeatureGroup")["Parameter"]["vlaue"]}"""


def start_pipeline(ds, **kwargs):
    hook = AwsLambdaHook(
        function_name=lambda_function,
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        log_type="None",
        qualifier="$LATEST",
        config=None
    )
    request = hook.invoke_lambda()
    response = json.loads(request["Payload"].read().decode())
    kwargs["ti"].xcom_push(
        key="ExecutionId",
        value=response["ExecutionId"]
    )