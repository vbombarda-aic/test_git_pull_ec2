from airflow import DAG
from datetime import date, timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from functions.class_mapping_ingestion import InsertApiData

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

# Dag is instanciated
with DAG('dag_mapping', start_date=datetime(2018, 10, 1), schedule_interval='@daily', default_args = DAG_DEFAULT_ARGS,
         catchup = False, is_paused_upon_creation=):

  task1 = InsertApiData(
    task_id="data_mapping_insertion",
    bucket_name = 'argo-data-lake',
    file_path = 'mapping/questions_categories_mapping.csv',
    table_name = 'mapping_table',
    db_credentials = db_credentials
  )
  
  (task1)
  
