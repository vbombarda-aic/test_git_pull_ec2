MERGE INTO SurveyQuestions AS A

USING(
    SELECT B."questionid", a."Question", a."Description",
            CONCAT_WS('_',B."questionid", a."Question") AS mergeKey

    FROM "mapping_table" a

    LEFT JOIN SurveyQuestions B ON A."Question" = B.question

) B

ON CONCAT_WS('_', A."questionid", A."question") = B.mergeKey

WHEN MATCHED THEN
    UPDATE SET "questiondescription" = B."Description"
