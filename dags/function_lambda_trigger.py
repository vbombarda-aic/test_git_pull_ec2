from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3

class TriggerLambdaOperator(BaseOperator):

    @apply_defaults
    def __init__(self, lambda_function_name, #aws_access_key_id, aws_secret_access_key,
                 aws_region_name='us-east-1', *args, **kwargs):
        super(TriggerLambdaOperator, self).__init__(*args, **kwargs)
        self.lambda_function_name = lambda_function_name
        # self.aws_access_key_id = aws_access_key_id
        # self.aws_secret_access_key = aws_secret_access_key
        # self.aws_region_name = aws_region_name

    def execute(self, context):
        # Create a Lambda client
        client = boto3.client('lambda',
                              # aws_access_key_id=self.aws_access_key_id,
                              # aws_secret_access_key=self.aws_secret_access_key,
                              region_name=self.aws_region_name)
        
        # Trigger the Lambda function
        response = client.invoke(FunctionName=self.lambda_function_name)
        
        # Check if invocation was successful
        if response['StatusCode'] == 200:
            self.log.info("Lambda function triggered successfully. Response:")
            response_payload = response['Payload'].read().decode('utf-8')
            print(response_payload)
        else:
            self.log.error("Error triggering Lambda function: %s", response)

        return response
