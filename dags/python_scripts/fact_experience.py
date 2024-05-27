import psycopg2
import pandas as pd
from psycopg2 import sql

def create_fact_experience(db_credentials):
    conn = psycopg2.connect(
                host=db_credentials["DB_HOST"],
                dbname=db_credentials["DB_NAME"],
                user=db_credentials["DB_USER"],
                password=db_credentials["DB_PASSWORD"],
                port=db_credentials["DB_PORT"])

    cur = conn.cursor()


    # Query to get distinct question descriptions
    cur.execute('''SELECT DISTINCT C."Category"
        FROM structured.SurveyAnswers A
        LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
        INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
        LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
        WHERE C."Category" NOT IN ('RespondentData', 'SurveyData', 'Attributes', 'Emotions')
        ORDER BY 1;''')
    columns = cur.fetchall()
    print('total of columns: ',len(columns))
    # Extract column names
    column_names = [row[0] for row in columns]

    # Create the column definition string for the crosstab query
    column_definitions = ", ".join([f'"{col}" DECIMAL' for col in column_names])

    # Query to get distinct question descriptions
    cur.execute('''SELECT DISTINCT B."questiondescription"
        FROM structured.SurveyAnswers A
        LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
        INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
        LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
        WHERE C."Category" IN ('Attributes', 'Emotions')
        ORDER BY 1;''')
    columns = cur.fetchall()
    print('total of columns: ',len(columns))
    # Extract column names
    column_names = [row[0].strip() + "_bool" for row in columns]

    # Create the column definition string for the crosstab query
    column_definitions2 = ", ".join([f'"{col}" INT' for col in column_names])

    # Construct the dynamic crosstab query
    crosstab_query = f"""
    CREATE EXTENSION IF NOT EXISTS tablefunc;

    DROP TABLE IF EXISTS analytics.fact_experience;

    CREATE TABLE analytics.fact_experience AS

    WITH ct AS (
        SELECT *

        FROM crosstab(
            $$
            SELECT "experienceid","Category", AVG("answer") FROM
                (SELECT D."experienceid", C."Category",

                CASE
                    WHEN A."answer" = 'Strongly disagree' THEN 1
                    WHEN A."answer" = 'Disagree' THEN 2
                    WHEN A."answer" = 'Somewhat disagree' THEN 3
                    WHEN A."answer" = 'Neither agree nor disagree' THEN 4
                    WHEN A."answer" = 'Somewhat agree' THEN 5
                    WHEN A."answer" = 'Agree' THEN 6
                    WHEN A."answer" = 'Strongly agree' THEN 7
                END answer

                FROM structured.SurveyAnswers A
                LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
                INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
                LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
                WHERE C."Category" NOT IN ('RespondentData', 'SurveyData', 'Attributes', 'Emotions')
                ORDER BY 1, 2)

                GROUP BY 1, 2
                $$,
                $$
                SELECT DISTINCT C."Category"
                FROM structured.SurveyAnswers A
                LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
                INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
                LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
                WHERE C."Category" NOT IN ('RespondentData', 'SurveyData', 'Attributes', 'Emotions')
                ORDER BY 1;
                $$
            ) AS ct (
                experienceid INT,
                {column_definitions}
            )
    ), ct2 AS (
        SELECT *

        FROM crosstab(
            $$
            SELECT D."experienceid", B."questiondescription",
                CASE
                    WHEN A."answer" LIKE '%NO TO%' THEN 0
                    ELSE 1
                END answer

                FROM structured.SurveyAnswers A
                LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
                INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
                LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
                WHERE C."Category" IN ('Attributes', 'Emotions')
                ORDER BY 1, 2

                $$,
                $$
                SELECT DISTINCT B."questiondescription"
                FROM structured.SurveyAnswers A
                LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
                INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
                LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
                WHERE C."Category" IN ('Attributes', 'Emotions')
                ORDER BY 1;
                $$
            ) AS ct2 (
                experienceid_2 INT,
                {column_definitions2}
            )
    )

    SELECT A."survey", A."content", A."timestamp", ct.*, ct2.*

    FROM structured.Experience A

    LEFT JOIN ct ON ct."experienceid" = A."experienceid"

    LEFT JOIN ct2 ON ct2."experienceid_2" = A."experienceid";

    ALTER TABLE analytics.fact_experience
    ADD CONSTRAINT fact_pk PRIMARY KEY ("experienceid", "content", "timestamp")
    

    """
    cur.execute(sql.SQL(crosstab_query))
    conn.commit()
    print("Table created successfully")
    # Close the cursor and connection
    cur.close()
    conn.close()
