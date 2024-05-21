from .function_data_ingestion import get_data, create_table_and_insert_data
from sqlalchemy import create_engine, text
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import pandas as pd

class PrintXCom(BaseOperator):

    @apply_defaults
    def __init__(self, name: str,
                    *args,
                    **kwargs):
        super(PrintXCom, self).__init__(*args, **kwargs)
        self.name = name
    
    def execute(self, context):
      ti = kwargs['ti']
      query_result = ti.xcom_pull(task_ids='retrieve_content_names', key='postgres_query_result')
      print("########################"
      print("Query result:", query_result)
      print("########################"
      return True
