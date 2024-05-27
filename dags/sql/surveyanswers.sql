MERGE INTO
    structured.SurveyAnswers AS A
USING (

    WITH exp_temp_view AS
    (
        SELECT A."RespondentID", A."variable" AS Question, A."value" AS Answer, B."experienceid"
        FROM temporary_table A
        LEFT JOIN structured.Experience B ON
                A."RespondentID" = B."respondentid"
                AND A."Survey" = B."survey"
                AND A."Content" = B."content"
    ), surv_quest_ans_view AS
    (
        SELECT B."questionid", A."variable" AS Question
        FROM temporary_table A
        LEFT JOIN SurveyQuestions B ON
                A."Survey" = B."survey"
                AND A."variable" = B."question"
    )
    SELECT DISTINCT B."questionid",
                    A."experienceid",
                    A."answer", 
                    CONCAT_WS('_', B."questionid", A."experienceid") AS mergeKey

    FROM exp_temp_view A

    LEFT JOIN surv_quest_ans_view B ON A."question" = B."question"

) B

ON CONCAT_WS('_', A.QuestionID, A.ExperienceID) = B.mergeKey

WHEN NOT MATCHED
THEN INSERT ("questionid", "experienceid", "answer")
VALUES (B."questionid", "experienceid", "answer");
