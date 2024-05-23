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


def transform_dict(data: dict or list, data_id, data_name: str, valueColumns: list = [], arrayColumns:list = []):
    if type(data) == dict:
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
        {table_name}_ID serial NOT NULL,
        ID int NOT NULL,
        Name text NOT NULL,
        {sql_create_table_script}
        CONSTRAINT {table_name}_pk PRIMARY KEY ({table_name}_ID, ID, Name)
         );'''
