from airflow.utils.decorators import apply_defaults
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
from functions.class_lambda_trigger import TriggerLambdaOperator
from functions.class_data_ingestion import InsertStructuredData
from functions.class_query_db import PostgresQueryOperator
from python_scripts.dim_respondents import create_dimension_respondents
from python_scripts.dim_content import create_dimension_content
from python_scripts.fact_experience import create_fact_experience
from airflow import DAG
import boto3
import os

# Database connection details
db_credentials = {
"DB_HOST":     'db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com',
"DB_NAME":     'argo',
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

def get_sql_file(file_name):
    sql_file_path = os.path.join(dag_directory, f'sql/{file_name}.sql')
    with open(sql_file_path, 'r') as file:
        return file.read()


# Get the directory of the current script file (the DAG file)
dag_directory = os.path.dirname(os.path.abspath(__file__))

content_sql = get_sql_file('content')
experience_sql = get_sql_file('experience')
survey_sql = get_sql_file('survey')
surveyquestions_sql = get_sql_file('surveyquestions')
surveyanswers_sql = get_sql_file('surveyanswers')

current_timestamp = datetime.now().strftime('%Y-%m-%dT%H:%M:%S') # f'{datetime.now():%Y-%m-%dT%H:%M:%S}'

# Define the DAG
with DAG('dag_main', default_args=default_args, description='DAG to trigger a Lambda function', schedule_interval=None,
         is_paused_upon_creation=False, start_date=datetime(2024, 5, 1), catchup=False) as dag:
    
    payload = {"bucket_name": "argo-data-lake",
               "file_path": "unvalidated/data_example.csv",
               "sep": ";"}
                        
    validate_task = TriggerLambdaOperator(
        task_id='data_validation_and_formatting',
        lambda_function_name='validate',
        payload=payload
    )

    ingest_task = InsertStructuredData(
        task_id='data_insertion_to_database',
        bucket_name="argo-data-lake",
        file_path="raw/processed_file.csv",
        table_name='temporary_table',
        db_credentials=db_credentials
    )
                        
    ## SQL commands to populate the tables
    # Content
    content_table = PostgresQueryOperator(
        task_id='content_table',
        sql_query=content_sql,
        db_credentials=db_credentials
    )

    # Experience
    experience_table = PostgresQueryOperator(
        task_id='experience_table',
        sql_query=experience_sql,
        db_credentials=db_credentials
    )

    # Survey
    survey_table = PostgresQueryOperator(
        task_id='survey_table',
        sql_query=survey_sql,
        db_credentials=db_credentials
    )

    # Survey Questions
    surveyquestions_table = PostgresQueryOperator(
        task_id='surveyquestions_table',
        sql_query=surveyquestions_sql,
        db_credentials=db_credentials
    )

    # Survey Answers
    surveyanswers_table = PostgresQueryOperator(
        task_id='surveyanswers_table',
        sql_query=surveyanswers_sql,
        db_credentials=db_credentials
    )

    ## Trigger a differente DAG pipeline
    api_trigger = TriggerDagRunOperator(
      task_id="trigger_api_dag",
      trigger_dag_id="dag_content_api_calls"
    )
             
    ## Trigger a differente DAG pipeline
    mapping_trigger = TriggerDagRunOperator(
      task_id="trigger_mapping_dag",
      trigger_dag_id="dag_mapping"
    )

    ## Run Python scripts that create the analytics tables
    users_dim = PythonOperator(
      task_id='users_dim',
      python_callable=create_dimension_respondents,
      op_kwargs={'db_credentials': db_credentials}
    )

    experience_fact = PythonOperator(
      task_id='experience_fact',
      python_callable=create_fact_experience,
      op_kwargs={'db_credentials': db_credentials}
    )

    content_dim = PythonOperator(
      task_id='content_dim',
      python_callable=create_dimension_content,
      op_kwargs={'db_credentials': db_credentials}
    )
    
    validate_task >> ingest_task
    ingest_task   >> content_table >> experience_table 
    ingest_task   >> survey_table >> surveyquestions_table
    [experience_table , surveyquestions_table] >> surveyanswers_table
    surveyanswers_table >> [api_trigger, mapping_trigger] >> users_dim >> content_dim >> experience_fact
    
