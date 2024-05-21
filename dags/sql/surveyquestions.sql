MERGE INTO
    SurveyQuestions AS A
USING (
    SELECT DISTINCT "Survey", "variable" AS Question,
            CONCAT_WS('_', "Survey", "variable") AS mergeKey

    FROM temporary_table

) B

ON CONCAT_WS('_', A.Survey, A.Question) = B.mergeKey

WHEN NOT MATCHED
THEN INSERT ("survey", "question")
VALUES (B."Survey", b."question");