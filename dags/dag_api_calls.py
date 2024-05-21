
from airflow.utils.decorators import apply_defaults
from airflow import DAG
from datetime import date, timedelta, datetime
import boto3
from functions.class_query_db import PostgresQueryOperator
from functions.class_print_xcom import PrintXCom

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
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

sql_script = '''
SELECT DISTINCT "content" FROM Content
'''

# Define the DAG
with DAG('dag_api_calls', default_args=default_args, description='DAG to trigger a Lambda function and ingest API data', schedule_interval='@daily',
                    start_date=datetime(2024, 5, 1), catchup=False) as dag:

    payload = {"bucket_name": "aws-bix-aicollaborator", "file_path": "template_example.csv"}

      
    content_table = PostgresQueryOperator(
        task_id='retrieve_content_names',
        sql_query=sql_script,
        db_credentials=db_credentials
    )

    print_content = PrintXCom(
        task_id='print_content_names',
        name="Airflow"
    )

    ( content_table >> print_content )

