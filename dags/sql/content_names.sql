WITH oc_reviews AS
(
    SELECT "id", "name", MAX("insertion_date") as insertion_date

    FROM structured.opencritic_reviews

    GROUP BY "id", "name"

), oc_info AS
(
    SELECT "id", "name", MAX("insertion_date") as insertion_date

    FROM structured.opencritic_info

    GROUP BY "id", "name"

), steam_reviews AS
(
    SELECT "id", "name", MAX("insertion_date") as insertion_date

    FROM structured.steam_reviews

    GROUP BY "id", "name"

), steam_info AS
(
    SELECT "id", "name", MAX("insertion_date") as insertion_date

    FROM structured.steam_info

    GROUP BY "id", "name"

)

SELECT DISTINCT A."contentid", A."content"

FROM structured.Content A

LEFT JOIN oc_reviews    B ON A."contentid" = B."id" AND A."content" = B."name"

LEFT JOIN oc_info       C ON A."contentid" = C."id" AND A."content" = C."name"

LEFT JOIN steam_reviews D ON A."contentid" = D."id" AND A."content" = D."name"

LEFT JOIN steam_info    E ON A."contentid" = E."id" AND A."content" = E."name"

WHERE EXTRACT(DAY FROM (B."insertion_date" - CURRENT_TIMESTAMP)) < -10

    OR EXTRACT(DAY FROM (C."insertion_date" - CURRENT_TIMESTAMP)) < -10

    OR EXTRACT(DAY FROM (D."insertion_date" - CURRENT_TIMESTAMP)) < -10

    OR EXTRACT(DAY FROM (E."insertion_date" - CURRENT_TIMESTAMP)) < -10

    OR (B."insertion_date" IS NULL AND C."insertion_date" IS NULL AND D."insertion_date" IS NULL AND E."insertion_date" IS NULL);
