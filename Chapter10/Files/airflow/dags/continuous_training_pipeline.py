import time
import json
import sagemaker
import boto3
import numpy as np
import pandas as pd
from time import sleep
from datetime import timedelta
from sagemaker.feature_store.feature_group import FeatureGroup

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.providers.amazon.aws.sensors.s3_prefix import S3PrefixSensor

sagemaker_session = sagemaker.Session()
region_name = sagemaker_session.boto_region_name
data_prefix = "abalone_data"
data_bucket = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="DataBucket")["Parameter"]["Value"]}"""
lambda_function = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="ReleaseChangeLambda")["Parameter"]["Value"]}"""
fg_name = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="FeatureGroup")["Parameter"]["Value"]}"""
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2)
}


def start_pipeline():
    hook = AwsLambdaHook(
        function_name=lambda_function,
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        log_type="Tail",
        qualifier="$LATEST",
        config=None
    )
    request = hook.invoke_lambda(payload="null")
    response = json.loads(request["Payload"].read().decode())
    print(f'ExecutionId: {response["ExecutionId"]}')


def update_feature_group():
    fg = FeatureGroup(name=fg_name, sagemaker_session=sagemaker_session)
    column_names = ["sex", "length", "diameter", "height", "whole_weight", "shucked_weight", "viscera_weight", "shell_weight", "rings"]
    abalone_data = pd.read_csv(f"s3://{data_bucket}/{data_prefix}/abalone.new", names=column_names)
    data = abalone_data[["rings", "sex", "length", "diameter", "height", "whole_weight", "shucked_weight", "viscera_weight", "shell_weight"]]
    processed_data = pd.get_dummies(data)
    time_stamp = int(round(time.time()))
    processed_data["TimeStamp"] = pd.Series([time_stamp] * len(processed_data), dtype="float64")
    fg.ingest(data_frame=processed_data, max_workers=5, wait=True)
    sleep(300)


with DAG(
    dag_id=f"acme-data-workflow",
    default_args=default_args,
    schedule_interval="@daily",
    concurrency=1,
    max_active_runs=1,
) as dag:

    s3_trigger = S3PrefixSensor(  
        task_id="s3_trigger",
        bucket_name=data_bucket,
        prefix=data_prefix,
        dag=dag
    )
    
    update_fg_task = PythonOperator(
        task_id="update_fg",
        python_callable=update_feature_group,
        dag=dag
    )
    
    trigger_release_task = PythonOperator(
        task_id="trigger_release_change",
        python_callable=start_pipeline,
        dag=dag
    )
    
    s3_trigger >> update_fg_task >> trigger_release_task