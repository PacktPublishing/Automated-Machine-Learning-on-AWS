import os
import logging
import json
import boto3
from botocore.exceptions import ClientError
from http import HTTPStatus
import numpy as np
import pandas as pd
from sklearn.preprocessing import normalize

sm = boto3.client("sagemaker-runtime")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(request, context):
    logger.info(f"Processing HTTP API Request: {json.dumps(request, indent=2)}")
    if request["requestContext"]["http"]["method"] == "POST":
        response_code, response_body = handle_request(request)
        return generate_response(request, response_body, response_code)
    else:
        logger.info("Request is not using POST method")
        return generate_response(request, json.dumps({"message:" "Unsupported method."}), HTTPStatus.BAD_REQUEST)


def generate_response(request, response_body, response_code):
    logger.info("Generating response:")
    response = {
        "body": response_body,
        "isBase64Encoded": request["isBase64Encoded"],
        "headers": request["headers"],
        "statusCode": response_code
    }
    logger.info(json.dumps(response, indent=2))
    return response


def handle_request(request):
    if request["rawPath"] == "/api/contact":
        logger.info("Processing Contact Form request.")
        return handle_contact(request)
    elif request["rawPath"] == "/api/predict":
        logger.info("Processing Prediction Form request.")
        return handle_predict(request)
    else:
        logger.info("Request outside of scope.")
        return HTTPStatus.BAD_REQUEST, json.dumps({"message": "Unsupported path."})


def handle_contact(request):
    email = json.loads(request["body"])["email"]
    return HTTPStatus.OK, json.dumps(
        {
            "message": f"<b>Thank you!</b> We\'ve received your message from <b>{email}</b> and, we will respond shortly."
        }
    )


def handle_predict(request):
    df = pd.json_normalize(json.loads(request["body"]))
    logger.info(f"Received Request Body: {df}")
    s_pre = df["sex"][0]
    s_post = handle_encoding(s_pre)
    x = df.drop(columns=["sex"], axis=1)
    x_pre = x.to_numpy()
    x_post = normalize(x_pre).tolist()[0]
    payload = ",".join(map(str, x_post+s_post))
    logger.info(f"SageMaker Request Payload: {payload}")
    try:
        if ("inference-id" in request["headers"]):
            inference_id = request["headers"]["inference-id"]
            logger.info(f"Invoking SageMaker Endpoint with Ground Truth Inference ID: {inference_id}")
            response = sm.invoke_endpoint(
                EndpointName=os.environ["sagemakerEndpoint"],
                ContentType="text/csv",
                Body=payload,
                InferenceId=inference_id
            )
        else:
            logger.info("Invoking SageMaker Enspoint with no Ground Truth Inference ID")
            response = sm.invoke_endpoint(
                EndpointName=os.environ["sagemakerEndpoint"],
                ContentType="text/csv",
                Body=payload
            )
        logger.debug(f"Sagemaker Response: {response}")
        prediction = response["Body"].read().decode("utf-8").split(".")[0]
        logger.info(f"SageMaker Endpoint Prediction: {prediction}")
        logger.debug(type(prediction))
        rings = round(int(prediction))
        age = rings * 1.5
        return HTTPStatus.OK, json.dumps(
            {
                "message": f"We\'ve calcuated that the Abalone has <b>{rings}</b> rings, and is therefore approximately <b>{age}</b> years old."
            }
        )
    
    except ClientError as e:
        error_message = e.response["Error"]["Message"]
        logger.error(error_message)
        return HTTPStatus.OK, json.dumps(
            {
                "message": "<b>Age Calculator Unavailable!</b> Please try again later."
            }
        )


def handle_encoding(sex):
    if sex == "M" or sex == "m":
        return [0., 0., 1.0]
    elif sex == "F" or sex == "f":
        return [1.0, 0., 0.]
    elif sex == "I" or sex == "i":
        return [0., 1.0, 0.]
