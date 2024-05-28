import boto3
import pandas as pd
from io import StringIO
import psycopg2

def get_data(bucket_name, file_key, sep=','):
    session = boto3.Session()
    s3 = session.client('s3')
    print('bucket_name ', bucket_name)
    print('file_key ', file_key)
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    data = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data), sep=sep)
    return df


def create_table_and_insert_data(df, engine, table_name):
    with engine.connect() as connection:
        df.head(0).to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        df.to_sql(name=table_name, con=engine, index=False, if_exists='append')

def formatDictionary(dictionary, expected_keys):
    for key in expected_keys:
        if key not in list(dictionary.keys()):
            dictionary[key] = None
    return dictionary
    
def transform_dict(data: dict or list, data_id, data_name: str, valueColumns: list = [], arrayColumns:list = []):
    if type(data) == dict:
        data = formatDictionary(data, valueColumns + arrayColumns)
        # Selects only the interested Columns
        data = {key: data[key] for key in valueColumns + arrayColumns}
        
        # Transform dictionries into lists
        for column in arrayColumns:
            try:
                data[column] = [x["name"] for x in data[column]]
            except:
                data[column] = [x["description"] for x in data[column]]
        
        data['id'] = data_id
        data['name'] = data_name
        return [data]
    
    elif type(data) == list:
        result_list = []
        for review in data:
            # Selects only the interested Columns
            review = formatDictionary(review, valueColumns + arrayColumns)
            final_dict = {key: review[key] for key in valueColumns + arrayColumns}
            
            # Transform dictionries into lists
            for column in arrayColumns:
                try:
                    final_dict[column] = [x["name"] for x in final_dict[column]]
                except:
                    final_dict[column] = [x["description"] for x in final_dict[column]]
            
            final_dict['id'] = data_id
            final_dict['name'] = data_name
            result_list.append(final_dict)
        return result_list
        
    else:
        raise Exception("Data object has to be of type list or dict.")


def create_script_table(table_name, valueColumns, arrayColumns):
    sql_create_table_script = """
    """
    for column in valueColumns:
        sql_create_table_script = sql_create_table_script + f"{column} text," + "\n"

    for column in arrayColumns:
        sql_create_table_script = sql_create_table_script + f"{column} text[],"  + "\n"


    return f'''
    CREATE TABLE IF NOT EXISTS {table_name} (
        {table_name.split('.')[-1]}_ID serial NOT NULL,
        ID int NOT NULL,
        Name text NOT NULL,
        {sql_create_table_script}
        Insertion_date timestamp NOT NULL,
        CONSTRAINT fk_contentid FOREIGN KEY (ID) REFERENCES structured.content(contentid)
         );'''


def list_s3_contents(bucket_name, folder_name):
    # Create a session using default credentials and configuration
    session = boto3.Session()
    s3 = session.client('s3')

    # Initialize the paginator for listing objects
    paginator = s3.get_paginator('list_objects_v2')

    # List all objects in the specified bucket and folder
    response_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_name)

    folders = []
    files = []

    for response in response_iterator:
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']

                # If the key ends with '/', it's a folder
                if key.endswith('/'):
                    folders.append(key)
                else:
                    files.append(key)
    return folders, files
