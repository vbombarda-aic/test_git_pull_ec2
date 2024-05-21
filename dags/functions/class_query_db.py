import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2

class PostgresQueryOperator(BaseOperator):
    @apply_defaults
    def __init__(self, sql_file_path: str,
                     db_credentials: dict,
                     *args,
                     **kwargs):
        super(PostgresQueryOperator, self).__init__(*args, **kwargs)
        self.sql_file_path = sql_file_path
        self.db_credentials = db_credentials

    def execute(self, context):
        # Read the SQL script
        if not os.path.isfile(self.sql_file_path):
            raise FileNotFoundError(f"SQL file not found: {self.sql_file_path}")
        
        with open(self.sql_file_path, 'r') as file:
            sql_query = file.read()

        # Extract database credentials
        db_host = self.db_credentials['DB_HOST']
        db_name = self.db_credentials['DB_NAME']
        db_user = self.db_credentials['DB_USER']
        db_password = self.db_credentials['DB_PASSWORD']
        db_port = self.db_credentials['DB_PORT']

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )
        cursor = conn.cursor()

        try:
            # Execute the SQL query
            cursor.execute(sql_query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.log.error(f"Error executing query: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
            self.log.info(f"Query executed successfully: {self.sql_file_path}")
