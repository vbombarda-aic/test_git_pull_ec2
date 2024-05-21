from airflow.utils.decorators import apply_defaults
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow import DAG
from datetime import date, timedelta, datetime
import boto3
import os
from functions.class_lambda_trigger import TriggerLambdaOperator
from functions.class_data_ingestion import InsertStructuredData
from functions.class_query_db import PostgresQueryOperator

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


# Get the directory of the current script file (the DAG file)
dag_directory = os.path.dirname(os.path.abspath(__file__))
sql_file_path = os.path.join(dag_directory, 'sql/content.sql')
with open(sql_file_path, 'r') as file:
    sql_script = file.read()
    
# sql_script = '''
# MERGE INTO
#     Content AS A
# USING (
#     SELECT DISTINCT "Content",
#             CONCAT_WS('_', "Content") AS mergeKey

#     FROM temporary_table
# sql_file_path = 'sql/content.sql'
# with open(sql_file_path, 'r') as file:
#     sql_script = file.read()

# ) B
# ON CONCAT_WS('_', A.Content) = B.mergeKey
# WHEN NOT MATCHED
# THEN INSERT ("content")
# VALUES (B."Content");
# '''

current_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S') # f'{datetime.now():%Y-%m-%dT%H:%M:%S}'

# Define the DAG
with DAG('dag_main', default_args=default_args, description='DAG to trigger a Lambda function', schedule_interval='@daily',
                    start_date=datetime(2024, 5, 1), catchup=False) as dag:
    
    payload = {"bucket_name": "argo-data-lake",
               "file_path": "unvalidated/data_example.csv"}
                        
    validate_task = TriggerLambdaOperator(
        task_id='data_validation_and_formatting',
        lambda_function_name='validate',
        payload=payload
    )

    ingest_task = InsertStructuredData(
        task_id='data_insertion_to_database',
        bucket_name="argo-data-lake",
        file_path="raw/processed_file.csv"
    )
    content_table = PostgresQueryOperator(
        task_id='content_table_ingestion',
        sql_query=sql_script,
        db_credentials=db_credentials
    )
    example_trigger = TriggerDagRunOperator(
      task_id="get_api_content",
      trigger_dag_id="dag_api_calls"
    )
    
    ( validate_task >> ingest_task >> content_table >> example_trigger)
    
