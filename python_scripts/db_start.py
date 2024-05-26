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

db_structured = "structured"
db_analytics = "analytics"
try:
  cursor.execute(sql.SQL(f"CREATE DATABASE {db_structured}"))
  print(f"{db_structured} successfully created!")
except:
  print(f"Error creating {db_structured} database.")

try:
  cursor.execute(sql.SQL(f"CREATE DATABASE {db_analytics}"))
  print(f"{db_analytics} successfully created!")
except:
  print(f"Error creating {db_analytics} database.")
