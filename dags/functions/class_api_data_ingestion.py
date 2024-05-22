from .function_data_ingestion import transform_dict
from sqlalchemy import create_engine, text
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import boto3
import pandas as pd
from io import StringIO

def create_table_and_insert_data(df, engine, table_name): #, dtype):
    with engine.connect() as connection:
        #try:
          #df.head(0).to_sql(name=table_name, con=engine, index=False, if_exists='fail') #, dtype=dtype)
        #except:
          #pass
        df.head(0).to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        df.to_sql(name=table_name, con=engine, index=False, if_exists='append')

def get_data(bucket_name, file_key):
    session = boto3.Session()
    s3 = session.client('s3')
    print('bucket_name ', bucket_name)
    print('file_key ', file_key)
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    return data

class InsertApiData(BaseOperator):

    @apply_defaults
    def __init__(self, bucket_name: str,
                    file_path: str,
                    db_credentials: dict, 
                    *args,
                    **kwargs):
        super(InsertApiData, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_path = file_path
        self.db_credentials = db_credentials
    
    def execute(self, context):
      execution_date = context['execution_date'].strftime('%Y-%m-%dT%H:%M:%S')
      file_path_split = self.file_path + 'review_and_details/' + str(execution_date) + '/1/content.json' # ITERATES THROUGH THE IDs IN THE 1 ! 
      data = get_data(self.bucket_name, file_path_split)
      data = json.loads(data)

      # Open Critic Info
      valueColumns = ['percentRecommended', 'numReviews', 'medianScore', 'topCriticScore','tier', 'description']
      arrayColumns = ['Companies', 'Genres']
      data_oc_info = transform_dict(data['oc_info'], data['id'], data['name'], valueColumns=valueColumns, arrayColumns=arrayColumns)
      df_oc_info = pd.DataFrame(data_oc_info)  
        
      # Open Critic Reviews
      valueColumns = ['score', 'language', 'publishedDate', 'snippet', 'externalUrl']
      data_oc_reviews = transform_dict(data['oc_reviews'], data['id'], data['name'], valueColumns=valueColumns)
      df_oc_reviews = pd.DataFrame(data_oc_reviews)  

      # Steam Info
      valueColumns = ['short_description']
      arrayColumns = ['categories', 'genres']
      game_id = list(data['steam_info'].keys())[0]
      data_steam_info = transform_dict(data['steam_info'][game_id]['data'], data['id'], data['name'],
                             valueColumns=valueColumns, arrayColumns=arrayColumns)
      df_steam_info= pd.DataFrame(data_steam_info)
    
      # Steam Reviews
      valueColumns = ['language', 'review', 'voted_up','votes_up','votes_funny', 'timestamp_created', 'timestamp_updated']
      data_steam_reviews = transform_dict(data['steam_reviews']['reviews'], data['id'], data['name'], valueColumns=valueColumns)
      df_steam_reviews = pd.DataFrame(data_steam_reviews)

      ### FINISH
        
      ##### Proceed to ingest it into the Database
      
      # Database connection details
      DB_HOST = self.db_credentials["DB_HOST"]
      DB_NAME = self.db_credentials["DB_NAME"]
      DB_USER = self.db_credentials["DB_USER"]
      DB_PASSWORD = self.db_credentials["DB_PASSWORD"]
      DB_PORT = self.db_credentials["DB_PORT"]
      # TABLE_NAME = 'temporary_table' - MULTIPLE different tables !!!
      engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')

      print("engine created")
      create_table_and_insert_data(df_steam_info, engine, "steamInfo")
      print("table created")

      return True
