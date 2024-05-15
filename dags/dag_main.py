from airflow.utils.decorators import apply_defaults
from airflow import DAG
from datetime import date, timedelta, datetime
import boto3
from functions.class_lambda_trigger import TriggerLambdaOperator
from functions.class_data_ingestion import InsertStructuredData



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

    payload = {"bucket_name": "argo-data-lake", "file_path": "unvalidated/data_example.csv"}
                        
    trigger_lambda = TriggerLambdaOperator(
        task_id='trigger_lambda_task',
        lambda_function_name='validate',
        payload=payload
    )

    ingest_airflow = InsertStructuredData(
        task_id='insertion_task',
        bucket_name = "argo-data-lake",
        file_path = "raw/data_example.csv"
    )
    
    ( trigger_lambda >> ingest_airflow )
    
