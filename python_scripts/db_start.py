import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(
            host="db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com",
            dbname="postgres",
            user="test_admin",
            password="test_password",
            port=5432)

conn.autocommit = True 
cursor = conn.cursor()
print("Connection successful")

db_name = "structured"
try:
cursor.execute(sql.SQL(f"CREATE DATABASE {db_name}"))
  print(f"{db_name} successfully created!")
except:
  print("Error creating {db_name} database.")
