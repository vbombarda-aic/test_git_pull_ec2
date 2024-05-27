import psycopg2
import pandas as pd
from psycopg2 import sql

def create_dimension_content(db_credentials):
    conn = psycopg2.connect(
                host=db_credentials["DB_HOST"],
                dbname=db_credentials["DB_NAME"],
                user=db_credentials["DB_USER"],
                password=db_credentials["DB_PASSWORD"],
                port=db_credentials["DB_PORT"])

    cur = conn.cursor()

    query = '''
    DROP TABLE IF EXISTS analytics.dim_content;

    CREATE TABLE analytics.dim_content AS
    WITH oc_info AS
    (
        SELECT "id",
                "name",
                "percentrecommended",
                "topcriticscore",
                "numReviews",
                "medianScore",
                "tier" as opencritic_tier,
                "companies",
                "genres" as oc_genres,
                "insertion_date",
                ROW_NUMBER() OVER (PARTITION BY "id", "name" ORDER BY "insertion_date" DESC) AS rn
                
        FROM structured.opencritic_info

    ), steam_info AS
    (
        SELECT "id",
                "name",
                "short_description",
                "categories",
                "genres" as steam_genres,
                "insertion_date",
                ROW_NUMBER() OVER (PARTITION BY "id", "name" ORDER BY "insertion_date" DESC) AS rn
                
        FROM structured.steam_info
    )
                
    SELECT A."id",
        A."name",
        B."short_description",
        B."categories",
        B."steam_genres",
        A."numReviews",
        A."medianScore",
        A."opencritic_tier",
        A."percentrecommended",
        A."topcriticscore",
        A."companies",
        A."oc_genres"
                
    FROM oc_info A
                
    LEFT JOIN steam_info B ON A."id" = B."id" AND A."name" = B."name"
                
    WHERE A."rn" = 1 AND B."rn" = 1;
    '''
    cur.execute(sql.SQL(query))
    cur.close()
    conn.close()
