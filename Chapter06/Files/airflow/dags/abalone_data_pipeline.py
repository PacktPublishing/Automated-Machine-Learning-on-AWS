import boto3
import json
from datetime import timedelta

import sagemaker
from sagemaker.tensorflow import TensorFlow
from sagemaker.tensorflow.serving import Model
from sagemaker.processing import ProcessingInput, ProcessingOutput, Processor
from sagemaker.model_monitor import DataCaptureConfig

import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.providers.amazon.aws.hooks.lambda_function import AwsLambdaHook
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

region_name = "us-west-2"
model_name = "abalone"
data_prefix = "abalone_data"
data_bucket = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="AirflowDataBucket")["Parameter"]["Value"]}"""
glue_job_name = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="GlueJob")["Parameter"]["Value"]}"""
crawler_name = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="GlueCrawler")["Parameter"]["Value"]}"""
sagemaker_role = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="SageMakerRoleARN")["Parameter"]["Value"]}"""
lambda_function = f"""{boto3.client("ssm", region_name=region_name).get_parameter(Name="AnalyzeResultsLambda")["Parameter"]["Value"]}"""
container_image = f"763104351884.dkr.ecr.{region_name}.amazonaws.com/tensorflow-training:2.5.0-cpu-py37-ubuntu18.04-v1.0"
training_input = f"s3://{data_bucket}/{data_prefix}/training"
testing_input = f"s3://{data_bucket}/{data_prefix}/testing"
data_capture = f"s3://{data_bucket}/endpoint-data-capture"
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2)
}


def training(data, **kwargs):
    estimator = TensorFlow(
        base_job_name=model_name,
        entry_point="/usr/local/airflow/dags/model/model_training.py",
        role=sagemaker_role,
        framework_version="2.4",
        py_version="py37",
        hyperparameters={"epochs": 200, "batch-size": 8},
        script_mode=True,
        instance_count=1,
        instance_type="ml.m5.xlarge",
    )
    estimator.fit(data)
    kwargs["ti"].xcom_push(
        key="TrainingJobName",
        value=str(estimator.latest_training_job.name)
    )


def evaluation(ds, **kwargs):
    training_job_name = kwargs["ti"].xcom_pull(key="TrainingJobName")
    estimator = TensorFlow.attach(training_job_name)
    model_data = estimator.model_data,
    processor = Processor(
        base_job_name=f"{model_name}-evaluation",
        image_uri=container_image,
        entrypoint=[
            "python3",
            "/opt/ml/processing/input/code/evaluate.py"
        ],
        instance_count=1,
        instance_type="ml.m5.xlarge",
        role=sagemaker_role,
        max_runtime_in_seconds=1200
    )
    processor.run(
        inputs=[
            ProcessingInput(
                source=testing_input,
                destination="/opt/ml/processing/testing",
                input_name="input"
            ),
            ProcessingInput(
                source=model_data[0],
                destination="/opt/ml/processing/model",
                input_name="model"
            ),
            ProcessingInput(
                source="s3://{}/airflow/scripts/evaluate.py".format(data_bucket),
                destination="/opt/ml/processing/input/code",
                input_name="code"
            )
        ],
        outputs=[
            ProcessingOutput(
                source="/opt/ml/processing/evaluation",
                destination="s3://{}/{}/evaluation".format(data_bucket, data_prefix),
                output_name="evaluation"
            )
        ]
    )


def deploy_model(ds, **kwargs):
    training_job_name = kwargs["ti"].xcom_pull(key="TrainingJobName")
    estimator = TensorFlow.attach(training_job_name)
    model = Model(
        model_data=estimator.model_data,
        role=sagemaker_role,
        framework_version="2.4",
        sagemaker_session=sagemaker.Session()
    )
    model.deploy(
        initial_instance_count=2,
        instance_type="ml.m5.large",
        data_capture_config=DataCaptureConfig(
            enable_capture=True,
            sampling_percentage=100,
            destination_s3_uri=data_capture
        )
    )


def get_results(ds, **kwargs):
    hook = AwsLambdaHook(
        function_name=lambda_function,
        aws_conn_id="aws_default",
        invocation_type="RequestResponse",
        log_type="None",
        qualifier="$LATEST",
        config=None
    )
    request = hook.invoke_lambda(
        payload=json.dumps(
            {
                "Bucket": data_bucket,
                "Key": f"{data_prefix}/evaluation/evaluation.json"
            }
        )
    )
    response = json.loads(request["Payload"].read().decode())
    kwargs["ti"].xcom_push(
        key="Results",
        value=response["Result"]
    )


def branch(ds, **kwargs):
    result = kwargs["ti"].xcom_pull(key="Results")
    if result > 3.1:
        return "rejected"
    else:
        return "approved"


with DAG(
    dag_id=f"{model_name}-data-workflow",
    default_args=default_args,
    schedule_interval="@daily",
    concurrency=1,
    max_active_runs=1,
) as dag:
    
    crawler_task = AwsGlueCrawlerOperator(
        task_id="crawl_data",
        config={"Name": crawler_name}
    )

    etl_task = AwsGlueJobOperator(
        task_id="preprocess_data",
        job_name=glue_job_name
    )

    training_task = PythonOperator(
        task_id="training",
        python_callable=training,
        op_args=[training_input],
        provide_context=True,
        dag=dag
    )

    evaluation_task = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluation,
        provide_context=True,
        dag=dag
    )

    analyze_results_task = PythonOperator(
        task_id="analyze_results",
        python_callable=get_results,
        provide_context=True,
        dag=dag
    )

    check_threshold_task = BranchPythonOperator(
        task_id="check_threshold",
        python_callable=branch,
        provide_context=True,
        dag=dag
    )

    deployment_task = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_model,
        provide_context=True,
        dag=dag
    )

    start_task = DummyOperator(
        task_id="start",
        dag=dag
    )

    end_task = DummyOperator(
        task_id="end",
        dag=dag
    )

    rejected_task = DummyOperator(
        task_id="rejected",
        dag=dag
    )

    approved_task = DummyOperator(
        task_id="approved",
        dag=dag
    )

    start_task >> crawler_task >> etl_task >> training_task >> evaluation_task >> analyze_results_task >> check_threshold_task >> [rejected_task, approved_task]
    approved_task >> deployment_task >> end_task
    rejected_task >> end_task