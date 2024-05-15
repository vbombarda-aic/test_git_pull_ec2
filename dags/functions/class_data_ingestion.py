from sqlalchemy import create_engine, text
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import boto3
import pandas as pd
from io import StringIO
import psycopg2

# Database connection details
DB_HOST = 'db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com'
DB_NAME = 'structured'
DB_USER = 'test_admin'
DB_PASSWORD = 'test_password'
DB_PORT = '5432'
TABLE_NAME = 'temporary_table'

UserID = "Respondent_ID"
DateTime = "Timestamp"
ContentID = "Content"
Survey = "Survey"

def get_data(bucket_name, file_key):
    session = boto3.Session()
    s3 = session.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    return df

def create_table_and_insert_data(df, engine, table_name):
    with engine.connect() as connection:
        df.head(0).to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        df.to_sql(name=table_name, con=engine, index=False, if_exists='append')

class InsertStructuredData(BaseOperator):

    @apply_defaults
    def __init__(self, bucket_name: str,
                    file_path: str,
                    *args,
                    **kwargs):
        super(ValidateInputtedData, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_path = file_path
    
    def execute(self, context):
        try:
          df = get_data(self.bucket_name, self.file_path)
          df = df.melt([UserID, DateTime, ContentID, Survey])
  
          engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
  
          print("engine created")
          create_table_and_insert_data(df, engine, TABLE_NAME)
          print("table created")
  
          return True
          
        except e:
          print(e)
          return False
