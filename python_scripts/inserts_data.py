import boto3
import pandas as pd
from io import StringIO
import psycopg2
from sqlalchemy import create_engine, text


# Replace these variables with your actual bucket name and file key
BUCKET_NAME = 'argo-data-lake'
FILE_KEY = 'raw/data_example.csv'

# Database connection details
DB_HOST = 'db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com'
DB_NAME = 'structured'
DB_USER = 'test_admin'
DB_PASSWORD = 'test_password'
DB_PORT = '5432'
TABLE_NAME = 'temporary_table'

def read_csv_from_s3(bucket_name, file_key):
    session = boto3.Session()
    s3 = session.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    return df
def create_table_and_insert_data(df, engine, table_name):
    with engine.connect() as connection:
        #connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
        #print("table dropped")
        df.head(0).to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        df.to_sql(name=table_name, con=engine, index=False, if_exists='append')


df = read_csv_from_s3(BUCKET_NAME, FILE_KEY)

UserID = "Respondent_ID"
DateTime = "Timestamp"
ContentID = "Content"
Survey = "Survey"

df = df.melt([UserID, DateTime, ContentID, Survey])

print(df.head(5))
engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
print("engine created")

create_table_and_insert_data(df, engine, TABLE_NAME)
print("table created")
