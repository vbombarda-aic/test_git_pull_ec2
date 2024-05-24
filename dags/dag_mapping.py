from airflow import DAG
from datetime import date, timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from functions.class_mapping_ingestion import InsertApiData
from functions.class_query_db import PostgresQueryOperator
import os

# Database connection details
db_credentials = {
"DB_HOST":     'db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com',
"DB_NAME":     'structured',
"DB_USER":     'test_admin',
"DB_PASSWORD": 'test_password',
"DB_PORT":     '5432'
}

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Get the directory of the current script file (the DAG file)
dag_directory = os.path.dirname(os.path.abspath(__file__))

def get_sql_file(file_name):
    sql_file_path = os.path.join(dag_directory, f'sql/{file_name}.sql')
    with open(sql_file_path, 'r') as file:
        return file.read()

update_sql = get_sql_file("update_questions_description")
create_categories_table_sql = get_sql_file("create_categories_table_sql")

# Dag is instanciated
with DAG('dag_mapping', start_date=datetime(2018, 10, 1), schedule_interval=None, is_paused_upon_creation=False, default_args = DAG_DEFAULT_ARGS,
         catchup = False):

  task1 = InsertApiData(
    task_id="data_mapping_insertion",
    bucket_name = 'argo-data-lake',
    file_path = 'mapping/questions_categories_mapping.csv',
    table_name = 'mapping_table',
    db_credentials = db_credentials
  ) 
  task2 = PostgresQueryOperator(
        task_id='update_questions_description',
        sql_query=update_sql,
        db_credentials=db_credentials
  )
  task3 = PostgresQueryOperator(
        task_id='create_categories_table',
        sql_query=create_categories_table_sql,
        db_credentials=db_credentials
  )
  
  (task1 >> [task2, task3])
  
