import os
import logging
import json
import boto3
import math
from botocore.exceptions import ClientError
from http import HTTPStatus

sm = boto3.client('sagemaker-runtime')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(request, context):
    logger.info(f'Processing HTTP API Request: {json.dumps(request, indent=2)}')
    if request['requestContext']['http']['method'] == 'POST':
        # Handle the request
        response_code, response_body = handle_request(request)
        return generate_response(request, response_body, response_code)
    else:
        # `POST` request is the only suppored method
        logger.info('Request is not using POST method')
        return generate_response(request, json.dumps({'message:' 'Unsupported method.'}), HTTPStatus.BAD_REQUEST)

def generate_response(request, response_body, response_code):
    # Generate response content
    logger.info('Generating response:')
    response = {
        'body': response_body,
        'isBase64Encoded': request['isBase64Encoded'],
        'headers': request['headers'],
        'statusCode': response_code

    }
    logger.info(json.dumps(response, indent=2))
    return response

def handle_request(request):
    # Determine request path and steer accordingly
    if request['rawPath'] == '/api/contact':
        logger.info('Processing Contact Form request.')
        return handle_contact(request)
    elif request['rawPath'] == '/api/predict':
        logger.info('Processing Prediction Form request.')
        return handle_predict(request)
    else:
        logger.info('Request outside of scope.')
        return HTTPStatus.BAD_REQUEST, json.dumps({'message': 'Unsupported path.'})

def handle_contact(request):
    """
    NOTE: This is a 'placeholder' response as this is a ficticious website.
    """
    # Return Form message
    email = json.loads(request['body'])['email']
    return HTTPStatus.OK, json.dumps(
        {
            'message': f'<b>Thank you!</b> We\'ve received your message from <b>{email}</b> and, we will respond shortly.'
        }
    )

def handle_predict(request):
    try:
        # Get request 'body'
        body = list(json.loads(request['body']).values())
        logger.info(f'Received Request Body: {body}')

        # Create a 'text/csv' payload
        payload = ','.join(map(str, body))
        logger.info(f'SageMaker Request Payload: {payload}')
        response = sm.invoke_endpoint(
            EndpointName=os.environ['sagemakerEndpoint'],
            ContentType='text/csv',
            Body=payload
        )
        logger.debug(f'Sagemaker Response: {response}')
        rings = response['Body'].read().decode('utf-8').split('.')[0]
        logger.info(f'SageMaker Endpoint Prediction: {rings}')
        logger.debug(type(rings))

        # Convert the predicted number of rings to Abalone age
        age = round(int(rings) * 1.5)

        # Return Form response message
        return HTTPStatus.OK, json.dumps(
            {
                'message': f'We\'ve calcuated that the Abalone has <b>{rings}</b> rings and, is therefore approximately <b>{age}</b> years old.'
            }
        )
    
    except ClientError as e:
        error_message = e.response['Error']['Message']
        logger.error(error_message)

        # Return Form resonse message
        return HTTPStatus.OK, json.dumps(
            {
                'message': '<b>Age Calculator Unavailable!</b> Please try again later.'
            }
        )
