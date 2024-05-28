MERGE INTO
    structured.Experience AS A
USING (
    
    SELECT DISTINCT C."RespondentID",
                    TO_TIMESTAMP(C."Timestamp", 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AS Timestamp,
                    C."Survey",
                    D."contentid",
                    CONCAT_WS('_', C."RespondentID",
                                   TO_TIMESTAMP(C."Timestamp", 'YYYY-MM-DD"T"HH24:MI:SS"Z"') AT TIME ZONE 'UTC',
                                   C."Survey",
                                   D."contentid") AS mergeKey

    FROM temporary_table C

    LEFT JOIN structured.content D ON C."Content" = D."content"

) B

ON CONCAT_WS('_', A.RespondentID, A.Timestamp, A.Survey, A.ContentID) = B.mergeKey

WHEN NOT MATCHED
THEN INSERT ("respondentid", "timestamp", "survey", "contentid")
VALUES (B."RespondentID", b."timestamp", B."Survey", B."contentid");
