MERGE INTO
    Experience AS A
USING (
    
    SELECT DISTINCT "RespondentID",
                    TO_TIMESTAMP("Timestamp", 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS Timestamp,
                    "Survey",
                    "Content",
                    CONCAT_WS('_', "RespondentID",
                                   TO_TIMESTAMP("Timestamp", 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AT TIME ZONE 'UTC',
                                   "Survey",
                                   "Content") AS mergeKey

    FROM temporary_table

) B

ON CONCAT_WS('_', A.RespondentID, A.Timestamp, A.Survey, A.Content) = B.mergeKey

WHEN NOT MATCHED
THEN INSERT ("respondentid", "timestamp", "survey", "content")
VALUES (B."RespondentID", b."timestamp", B."Survey", B."Content");