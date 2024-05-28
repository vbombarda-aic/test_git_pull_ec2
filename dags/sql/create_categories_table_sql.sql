DROP TABLE IF EXISTS structured.QuestionsCategories;

CREATE TABLE structured.QuestionsCategories (
  "Category" text,
  "Question" text,
  FOREIGN KEY ("Question") REFERENCES structured.SurveyQuestions("Question")
);

INSERT INTO structured.QuestionsCategories ("Category", "Question")
  SELECT A."Category", A."Question"
  FROM "mapping_table" AS A;
