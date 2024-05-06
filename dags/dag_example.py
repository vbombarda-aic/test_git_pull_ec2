from airflow import DAG
from datetime import date, timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator

DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

class HelloOperator(BaseOperator):
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message

# Dag is instanciated
with DAG('dag_example', start_date=datetime(2018, 10, 1), schedule_interval='@daily', default_args = DAG_DEFAULT_ARGS, catchup = False):

  task1 = HelloOperator(
    task_id="dag_example",
    name="Airflow"
  )
  
  (task1)
  
