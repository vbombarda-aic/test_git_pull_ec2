import boto3
import pandas as pd
from io import StringIO
import psycopg2

def get_data(bucket_name, file_key):
    session = boto3.Session()
    s3 = session.client('s3')
    print('bucket_name ', bucket_name)
    print('file_key ', file_key)
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    return df

def create_table_and_insert_data(df, engine, table_name):
    with engine.connect() as connection:
        df.head(0).to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        df.to_sql(name=table_name, con=engine, index=False, if_exists='append')
