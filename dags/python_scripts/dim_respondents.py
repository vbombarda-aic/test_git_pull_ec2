import psycopg2
import pandas as pd
from psycopg2 import sql

def create_dimension_respondents(db_credentials):
    conn = psycopg2.connect(
                host=db_credentials["DB_HOST"],
                dbname=db_credentials["DB_NAME"],
                user=db_credentials["DB_USER"],
                password=db_credentials["DB_PASSWORD"],
                port=db_credentials["DB_PORT"])

    cur = conn.cursor()


    # Query to get distinct question descriptions
    cur.execute('''SELECT DISTINCT B."questiondescription"
        FROM structured.SurveyAnswers A
        LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
        INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
        LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
        WHERE C."Category" = 'RespondentData'
        ORDER BY 1;''')
    columns = cur.fetchall()
    print('total of columns: ',len(columns))
    # Extract column names
    column_names = [row[0] for row in columns]

    # Create the column definition string for the crosstab query
    column_definitions = ", ".join([f'"{col}" TEXT' for col in column_names])

    # Construct the dynamic crosstab query
    crosstab_query = f"""
    CREATE EXTENSION IF NOT EXISTS tablefunc;

    DROP TABLE IF EXISTS analytics.dim_respondents;

    CREATE TABLE analytics.dim_respondents AS

    SELECT * FROM crosstab(
        $$
        SELECT D."experienceid", B."questiondescription", A."answer"
        FROM structured.SurveyAnswers A
        LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
        INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
        LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
        WHERE C."Category" = 'RespondentData'
        ORDER BY 1, 2;
        $$,
        $$
        SELECT DISTINCT B."questiondescription"
        FROM structured.SurveyAnswers A
        LEFT JOIN structured.SurveyQuestions B ON A."questionid" = B."questionid"
        INNER JOIN structured.QuestionsCategories C ON  B."question" = C."Question"
        LEFT JOIN structured.Experience D ON A."experienceid" = D."experienceid"
        WHERE C."Category" = 'RespondentData'
        ORDER BY 1;
        $$
    ) AS ct (
        id INT,
        {column_definitions}
    );

    ALTER TABLE analytics.dim_respondents
    ADD CONSTRAINT respondents_pk PRIMARY KEY ("experienceid");
    """
    cur.execute(sql.SQL(crosstab_query))
    conn.commit()
    print("Table created successfully")
    # Close the cursor and connection
    cur.close()
    conn.close()
