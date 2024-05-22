
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from datetime import date, timedelta, datetime
import boto3
import os
from functions.class_query_db import PostgresQueryOperator
from functions.class_lambda_trigger import TriggerLambdaOperator
from functions.class_api_data_ingestion import InsertApiData

# Database connection details
db_credentials = {
"DB_HOST":     'db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com',
"DB_NAME":     'structured',
"DB_USER":     'test_admin',
"DB_PASSWORD": 'test_password',
"DB_PORT":     '5432'
}

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

file_name = 'content_names'
dag_directory = os.path.dirname(os.path.abspath(__file__))
sql_file_path = os.path.join(dag_directory, f'sql/{file_name}.sql')
with open(sql_file_path, 'r') as file:
    sql_script = file.read()

# Define the DAG
with DAG('dag_content_api_calls', default_args=default_args, description='DAG to trigger a Lambda function and ingest API data', schedule_interval='@daily',
                    start_date=datetime(2024, 5, 1), catchup=False) as dag:

    payload = {'bucket_name': 'argo-data-lake', 'file_path': 'raw/api_data/'}

      
    content_table = PostgresQueryOperator(
        task_id='retrieve_content_names',
        sql_query=sql_script,
        db_credentials=db_credentials
    )

    trigger_lambda = TriggerLambdaOperator(
        task_id='trigger_content_api_calls',
        lambda_function_name='content_api_calls',
        payload=payload,
        extract_xcom=True,
        task_ids='retrieve_content_names',
        key='postgres_query_result'
    )

    transform_data = InsertApiData(
        task_id = 'transform_n_export_data',
        bucket_name = payload['bucket_name'],
        file_path = payload['file_path'],
        db_credentials = db_credentials
    )

    ( content_table >> trigger_lambda >> transform_data )
