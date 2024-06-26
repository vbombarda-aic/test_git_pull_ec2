from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import boto3

class TriggerLambdaOperator(BaseOperator):

    @apply_defaults
    def __init__(self, lambda_function_name: str,
                    payload: str,
                    extract_xcom: bool = False,
                    task_ids: str = "",
                    key: str = "",
                    aws_region_name:str = 'us-east-1',
                    *args,
                    **kwargs):
        super(TriggerLambdaOperator, self).__init__(*args, **kwargs)
        self.lambda_function_name = lambda_function_name
        self.aws_region_name = aws_region_name
        self.payload = payload
        self.extract_xcom = extract_xcom
        self.task_ids = task_ids
        self.key = key

    def execute(self, context):
        # Create a Lambda client
        client = boto3.client('lambda',
                              region_name=self.aws_region_name)
        execution_date = context['execution_date']
        self.payload["current_timestamp"] = execution_date.strftime('%Y-%m-%dT%H:%M:%S')

        if self.extract_xcom:
            ti = context['ti']  # Access the task instance from the context
            query_result = ti.xcom_pull(task_ids=self.task_ids, key=self.key)
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
