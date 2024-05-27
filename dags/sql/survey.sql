MERGE INTO
    structured.Survey AS A
USING (
    SELECT DISTINCT "Survey",
            CONCAT_WS('_', "Survey") AS mergeKey

    FROM temporary_table

) B

ON CONCAT_WS('_', A.Survey) = B.mergeKey

WHEN NOT MATCHED
THEN INSERT ("survey")
VALUES (B."Survey");