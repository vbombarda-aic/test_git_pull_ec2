import psycopg2
from psycopg2 import sql

## Create database
conn = psycopg2.connect(
            host="db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com",
            dbname="postgres",
            user="test_admin",
            password="password_test",
            port=5432)

conn.autocommit = True 
cursor = conn.cursor()
print("Connection successful")

db_name = "argo"
try:
  cursor.execute(sql.SQL(f"CREATE DATABASE {db_name}"))
  print(f"{db_name} successfully created!")
except:
  print(f"Error creating {db_name} database.")
cursor.close()
conn.close()

## Create schemas
conn = psycopg2.connect(
            host="db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com",
            dbname=db_name,
            user="test_admin",
            password="password_test",
            port=5432)

conn.autocommit = True 
cursor = conn.cursor()
print("Connection successful")

schemas = ["structured", "analytics"]
for schema in schemas:
   try:
     cursor.execute(sql.SQL(f"CREATE SCHEMA {schema}"))
     print(f"{schema} successfully created!")
   except:
     print(f"Error creating {schema} schema.")


