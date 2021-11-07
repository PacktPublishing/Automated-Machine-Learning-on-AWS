import os
import logging
import boto3
from botocore.exceptions import ClientError

sm = boto3.client("sagemaker")
ssm = boto3.client("ssm")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)

    if ("modelUri" in event):
        model_uri = event["modelUri"]
    else:
        raise KeyError("'Model Uri' not found in Lambda event!")
    
    if ("evaluationUri" in event):
        evaluation_uri = event["evaluationUri"]
    else:
        raise KeyError("'Evaluation File URI' not found in Lambda event!")
    
    if ("baselineUri" in event):
        baseline_uri = event["baselineUri"]
    else:
        raise KeyError("'Testing Data URI' not found in Lambda event!")
    
    if ("executionId" in event):
        execution_id = event["executionId"]
    else:
        raise KeyError("'Execition ID' not found in Lambda event!")

    request = {
        "InferenceSpecification": { 
            "Containers": [ 
                { 
                    "Image": os.environ["IMAGE_URI"],
                    "ModelDataUrl": model_uri
                }
            ],
            "SupportedContentTypes": [ 
                "text/csv" 
            ],
            "SupportedRealtimeInferenceInstanceTypes": [ 
                "ml.t2.large",
                "ml.c5.large",
                "ml.c5.xlarge"
            ],
            "SupportedResponseMIMETypes": [ 
                "text/csv" 
            ],
            "SupportedTransformInstanceTypes": [ 
                "ml.c5.xlarge"
            ]
        },
        "ModelApprovalStatus": "Approved",
        "MetadataProperties": {
            "ProjectId": execution_id,
            "GeneratedBy": "CDK Pipeline"
        },
        "ModelMetrics": {
            "ModelQuality": { 
                "Statistics": { 
                    "ContentType": "application/json",
                    "S3Uri": f"s3://{os.environ['BUCKET']}/{evaluation_uri}"
                }
            }
        },
        "ModelPackageDescription": "MLOps Production Model",
        "ModelPackageGroupName": os.environ["GROUP_NAME"]
    }

    try:
        logger.info("Creating model package.")
        response = sm.create_model_package(**request)
        model_package_arn = response["ModelPackageArn"]
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    
    try:
        logger.info("Updating SSM Parameter with the latest model package.")
        response = ssm.put_parameter(
            Name=os.environ["PACKAGE_PARAMETER"],
            Value=model_package_arn,
            Type="String",
            Overwrite=True
        )
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)
    
    try:
        logger.info("Creating SSM Parameter with the latest copy of the testing data.")
        response = ssm.put_parameter(
            Name=os.environ["BASELINE_PARAMETER"],
            Value=baseline_uri,
            Type="String",
            Overwrite=True
        )
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        raise Exception(error_message)

    logger.info("Done!")
    return {
        "statusCode": 200,
        "PackageArn": model_package_arn,
        "TestingParameter": "TestingDataUri"
    }
