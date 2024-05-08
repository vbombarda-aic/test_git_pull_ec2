
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from datetime import date, timedelta, datetime
import boto3
from function_lambda_trigger import TriggerLambdaOperator



# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG('dag_main', default_args=default_args, description='DAG to trigger a Lambda function', schedule_interval='@daily',
                    start_date=datetime(2024, 5, 1), catchup=False) as dag:


    trigger_lambda = TriggerLambdaOperator(
        task_id='trigger_lambda_task',
        lambda_function_name='hello_world',
        aws_access_key_id='secret',
        aws_secret_access_key='key-secret',
        aws_region_name='us-east-1'
    )

    ( trigger_lambda )
