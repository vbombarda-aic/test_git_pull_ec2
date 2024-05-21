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