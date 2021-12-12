import os
import json
import logging
import boto3
import math

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    logger.debug("## Environment Variables ##")
    logger.debug(os.environ)
    logger.debug("## Event ##")
    logger.debug(event)
    s3 = boto3.client("s3")
    if ("Bucket" in event):
        bucket = event["Bucket"]
    else:
        raise KeyError("S3 'Bucket' not found in Lambda event!")
    if ("Key" in event):
        key = event["Key"]
    else:
        raise KeyError("S3 'Key' not found in Lambda event!")
    logger.info("Downloading evlauation results file ...")
    json_file = json.loads(s3.get_object(Bucket = bucket, Key = key)['Body'].read())
    logger.info("Analyzing Model Evaluation Results ...")
    y = json_file["GroundTruth"]
    y_hat = json_file["Predictions"]
    summation = 0
    for i in range (0, len(y)):
        squared_diff = (y[i] - y_hat[i])**2
        summation += squared_diff
    rmse = math.sqrt(summation/len(y))
    logger.info("Root Mean Square Error: {}".format(rmse))
    logger.info("Done!")
    return {
        "statusCode": 200,
        "Result": rmse,
    }