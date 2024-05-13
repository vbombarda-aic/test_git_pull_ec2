from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import boto3

class TriggerLambdaOperator(BaseOperator):

    @apply_defaults
    def __init__(self, lambda_function_name: str,
                    payload: str,
                    aws_region_name:str = 'us-east-1',
                    *args,
                    **kwargs):
        super(TriggerLambdaOperator, self).__init__(*args, **kwargs)
        self.lambda_function_name = lambda_function_name
        self.aws_region_name = aws_region_name
        self.payload = payload

    def execute(self, context):
        # Create a Lambda client
        client = boto3.client('lambda',
                              region_name=self.aws_region_name)
        
        # Trigger the Lambda function
        response = client.invoke(FunctionName=self.lambda_function_name,
                                 Payload=json.dumps(self.payload))
        
        # Check if invocation was successful
        if response['StatusCode'] == 200:
            self.log.info("Lambda function triggered successfully. Response:")
            # response_payload = response['Payload'].read().decode('utf-8')
            response_payload = json.load(response['Payload'])
            message = response_payload['body']['message']
            print(response_payload)
        else:
            self.log.error("Error triggering Lambda function: %s", response)

        return response_payload
