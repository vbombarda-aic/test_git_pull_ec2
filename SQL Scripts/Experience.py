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
    Experience AS A
USING (
    SELECT DISTINCT "RespondentID",

    TO_TIMESTAMP("Timestamp" / 1000) AS Timestamp,

    "Survey", "Content",
            CONCAT_WS('_', "RespondentID", "Timestamp", "Survey", "Content") AS mergeKey

    FROM temporary_table

) B

ON CONCAT_WS('_', A.RespondentID, A.Timestamp, A.Survey, A.Content) = B.mergeKey

WHEN NOT MATCHED
THEN INSERT ("respondentid", "timestamp", "survey", "content")
VALUES (B."RespondentID", b."timestamp", B."Survey", B."Content");



''')
conn.commit()
cur.close()
conn.close()
