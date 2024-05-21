from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import boto3

class TriggerApiLambdaOperator(BaseOperator):

    @apply_defaults
    def __init__(self, lambda_function_name: str,
                    payload: str,
                    aws_region_name:str = 'us-east-1',
                    *args,
                    **kwargs):
        super(TriggerApiLambdaOperator, self).__init__(*args, **kwargs)
        self.lambda_function_name = lambda_function_name
        self.aws_region_name = aws_region_name
        self.payload = payload

    def execute(self, context):
        # Create a Lambda client
        client = boto3.client('lambda',
                              region_name=self.aws_region_name)
        
        # Execution date to create the folders
        execution_date = context['execution_date']

        # Extract the list of contents we want to retrieve
        ti = context['ti']  # Access the task instance from the context
        query_result = ti.xcom_pull(task_ids='retrieve_content_names', key='postgres_query_result')

        ## Insert the previous data to be passed to the lambda
        self.payload["current_timestamp"] = execution_date.strftime('%Y-%m-%dT%H:%M:%S')
        self.payload["content_names"] = query_result
        
        
        # Trigger the Lambda function
        response = client.invoke(FunctionName=self.lambda_function_name,
                                 Payload=json.dumps(self.payload))
        
        # Check if invocation was successful
        if response['StatusCode'] == 200:
            self.log.info("Lambda function triggered successfully. Response:")
            # response_payload = response['Payload'].read().decode('utf-8')
            response_payload = dict(json.loads(response['Payload'].read()))
            print(response_payload)
        else:
            self.log.error("Error triggering Lambda function: %s", response)

        if not response_payload["body"]["success"]:
            raise ValueError('The invoked Lambda Function failed.')

        return response_payload
