import os
import logging
import boto3
from botocore.exceptions import ClientError

sm = boto3.client("sagemaker")
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)
    props = event["ResourceProperties"]
    group_name = props["GroupName"]

    if event["RequestType"] == "Create":
        try:
            response = sm.create_model_package_group(
                ModelPackageGroupName=group_name,
                ModelPackageGroupDescription="Models Package Group for Production Models",
                Tags=[
                    {
                        "Key": "Name",
                        "Value": group_name
                    }
                ]
            )
            package_arn = response["ModelPackageGroupArn"]
            logger.info(f"Created Model Model Package Group: {package_arn}")
            return {
                "PhysicalResourceId": group_name,
                "Data": {
                    "ModelPackageArn": package_arn
                }
            }
        except ClientError as e:
            error_message = e.response["Error"]["Message"]
            logging.error(f"Failed to create Model Package Group: {error_message}")
            raise Exception(error_message)
    
    elif event["RequestType"] == "Delete":
        try:
            response = sm.list_model_packages(
                ModelPackageGroupName=group_name,
                ModelApprovalStatus="Approved",
                SortBy="CreationTime",
                MaxResults=100
            )

            for model_package in response["ModelPackageSummaryList"]:
                sm.delete_model_package(ModelPackageName=model_package["ModelPackageArn"])
            
            sm.delete_model_package_group(ModelPackageGroupName=group_name)
            logger.info(f"Deleted Model Package Group: {group_name}")
            return {
                "PhysicalResourceId": group_name,
                "Data":{}
            }
        
        except ClientError as e:
            error_message = e.response["Error"]["Messgae"]
            logger.error(f"Failed to delete Model Package Group: {error_message}")
            raise Exception(error_message)