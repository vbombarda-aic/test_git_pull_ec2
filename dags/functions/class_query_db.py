import os
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import psycopg2

class PostgresQueryOperator(BaseOperator):
    @apply_defaults
    def __init__(self, sql_query: str,
                     db_credentials: dict,
                     *args,
                     **kwargs):
        super(PostgresQueryOperator, self).__init__(*args, **kwargs)
        self.sql_query = sql_query
        self.db_credentials = db_credentials

    def execute(self, context):
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
            cursor.execute(self.sql_query)
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.log.error(f"Error executing query: {e}")
            raise
        finally:
            cursor.close()
            conn.close()
            self.log.info(f"Query executed successfully!")
