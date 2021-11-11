FROM public.ecr.aws/lambda/python:3.8
COPY index.py requirements.txt ./
RUN pip3 install -r requirements.txt
CMD ["index.lambda_handler"]