from .function_data_ingestion import get_data, create_table_and_insert_data
from sqlalchemy import create_engine, text
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import pandas as pd

class InsertApiData(BaseOperator):

    @apply_defaults
    def __init__(self, bucket_name: str,
                    file_path: str,
                    table_name: str,
                    db_credentials: dict,
                    *args,
                    **kwargs):
        super(InsertApiData, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.table_name = table_name
        self.db_credentials = db_credentials
    
    def execute(self, context):
      DB_HOST = self.db_credentials['DB_HOST']
      DB_NAME = self.db_credentials['DB_NAME']
      DB_USER = self.db_credentials['DB_USER']
      DB_PASSWORD = self.db_credentials['DB_PASSWORD']
      DB_PORT = self.db_credentials['DB_PORT']

      execution_date = context['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')
      file_path_split = self.file_path.split("/")
      df = get_data(self.bucket_name, self.file_path, sep=';')

      engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

      print("engine created")
      create_table_and_insert_data(df, engine, self.table_name)
      print("table created")

      return True
          
