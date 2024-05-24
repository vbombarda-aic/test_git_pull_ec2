DROP TABLE IF EXISTS QuestionsCategories;

CREATE TABLE QuestionsCategories AS

  SELECT A."Category", A."Question"

  FROM "mapping_table" AS A
