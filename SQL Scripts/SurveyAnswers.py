import psycopg2

conn = psycopg2.connect(
            host="db-postgres-aic-instance.cx82qoiqyhd2.us-east-1.rds.amazonaws.com",
            dbname="structured",
            user="test_admin",
            password="test_password",
            port=5432)

cur = conn.cursor()

#TEM QUE FAZER O JOIN DA temporary_table COM Experience (depende dela) = PARA CONSEGUR O ExperienceID !!!!

#VER SE FUNCIONA !!!!








cur.execute('''

WITH exp_temp_view AS
(
    SELECT A."RespondentID", A."variable" AS Question, A."value" AS Answer, B."ExperienceID"
    FROM temporary_view A
    LEFT JOIN Experience B ON
            A."RespondentID" = B."RespondentID"
            AND A."Survey" = B."Survey"
            AND A."Content" = B."Content"
), surv_quest_ans_view AS
(
    SELECT B."QuestionID", A."Question"
    FROM temporary_view A
    LEFT JOIN SurveyQuestions B ON
            A."Survey" = B."Survey"
            AND A."variable" = B."Question"
)
            
    SELECT B."QuestionID", A."ExperienceID", A."Answer"

    FROM exp_temp_view A
    
    LEFT JOIN surv_quest_ans_view B ON A."Question" = B."Question"

--- MERGE INTO
---     SurveyQuestions AS A
--- USING (
---     SELECT DISTINCT "Survey", "variable" AS Question, 
---             CONCAT_WS('_', "Survey") AS mergeKey
--- 
---     FROM temporary_table
--- 
--- ) B
--- 
--- ON CONCAT_WS('_', A.Survey, A.Question) = B.mergeKey
--- 
--- WHEN NOT MATCHED
--- THEN INSERT ("survey", "question")
--- VALUES (B."Survey", B."Question");

''')
# conn.commit()
cur.close()
conn.close()