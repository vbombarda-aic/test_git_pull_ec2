DROP TABLE IF EXISTS structured.QuestionsCategories;

CREATE TABLE structured.QuestionsCategories AS
  SELECT A."Category", A."Question"
  FROM "mapping_table" AS A
