import psycopg2

conn = psycopg2.connect(
            host="db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com",
            dbname="structured",
            user="test_admin",
            password="test_password",
            port=5432)

cur = conn.cursor()
cur.execute('''


MERGE INTO
    Content AS A
USING (
    SELECT DISTINCT "Content",
            CONCAT_WS('_', "Content") AS mergeKey

    FROM temporary_table

) B

ON CONCAT_WS('_', A.Content) = B.mergeKey

WHEN NOT MATCHED
THEN INSERT ("content")
VALUES (B."Content");

''')
conn.commit()
cur.close()
conn.close()
