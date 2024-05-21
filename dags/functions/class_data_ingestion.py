from .function_data_ingestion import get_data, create_table_and_insert_data
from sqlalchemy import create_engine, text
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import pandas as pd

# Database connection details
DB_HOST = 'db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com'
DB_NAME = 'structured'
DB_USER = 'test_admin'
DB_PASSWORD = 'test_password'
DB_PORT = '5432'
TABLE_NAME = 'temporary_table'

UserID = "RespondentID"
DateTime = "Timestamp"
ContentID = "Content"
Survey = "Survey"


class InsertStructuredData(BaseOperator):

    @apply_defaults
    def __init__(self, bucket_name: str,
                    file_path: str,
                    *args,
                    **kwargs):
        super(InsertStructuredData, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_path = file_path
    
    def execute(self, context):
      execution_date = context['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')
      file_path_split = self.file_path.split("/")
      enriched_file_path = file_path_split[0] + "/survey_data/" + str(execution_date) + "/" + file_path_split[1]
      df = get_data(self.bucket_name, enriched_file_path)
      df = df.melt([UserID, DateTime, ContentID, Survey])

      engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

      print("engine created")
      create_table_and_insert_data(df, engine, TABLE_NAME)
      print("table created")

      return True
          
