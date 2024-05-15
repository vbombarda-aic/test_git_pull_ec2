import psycopg2

# SQL script to create tables and constraints
sql_script = """
CREATE TABLE Content (
    ContentID serial  NOT NULL,
    ContentName text  NOT NULL,
    CONSTRAINT Content_pk PRIMARY KEY (ContentID)
);

-- Table: Experience
CREATE TABLE Experience (
    ExperienceID serial  NOT NULL,
    RespondentID text  NOT NULL,
    Timestamp timestamp  NOT NULL,
    Survey text  NOT NULL,
    ContentID int  NOT NULL,
    CONSTRAINT Experience_pk PRIMARY KEY (ExperienceID)
);

-- Table: Survey
CREATE TABLE Survey (
    SurveyID serial  NOT NULL,
    SurveyName text  NOT NULL,
    CONSTRAINT Survey_pk PRIMARY KEY (SurveyID)
);

-- Table: SurveyAnswers
CREATE TABLE SurveyAnswers (
    Answer text  NOT NULL,
    ExperienceID serial  NOT NULL,
    QuestionID serial  NOT NULL,
    CONSTRAINT SurveyAnswers_pk PRIMARY KEY (QuestionID,ExperienceID)
);

-- Table: SurveyQuestions
CREATE TABLE SurveyQuestions (
    SurveyID serial  NOT NULL,
    QuestionID serial  NOT NULL,
    Question text  NOT NULL,
    QuestionDescription text  NOT NULL,
    CONSTRAINT SurveyQuestions_pk PRIMARY KEY (QuestionID)
);

-- foreign keys
-- Reference: Experience_Content (table: Experience)
ALTER TABLE Experience ADD CONSTRAINT Experience_Content
    FOREIGN KEY (ContentID)
    REFERENCES Content (ContentID)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: SurveyAnswers_Experience (table: SurveyAnswers)
ALTER TABLE SurveyAnswers ADD CONSTRAINT SurveyAnswers_Experience
    FOREIGN KEY (ExperienceID)
    REFERENCES Experience (ExperienceID)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: SurveyAnswers_SurveyQuestions (table: SurveyAnswers)
ALTER TABLE SurveyAnswers ADD CONSTRAINT SurveyAnswers_SurveyQuestions
    FOREIGN KEY (QuestionID)
    REFERENCES SurveyQuestions (QuestionID)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;

-- Reference: SurveyQuestions_Survey (table: SurveyQuestions)
ALTER TABLE SurveyQuestions ADD CONSTRAINT SurveyQuestions_Survey
    FOREIGN KEY (SurveyID)
    REFERENCES Survey (SurveyID)  
    NOT DEFERRABLE 
    INITIALLY IMMEDIATE
;
"""

def create_tables(dbname, user, password, host, port):
    try:
        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        cursor = conn.cursor()

        # Execute the SQL script
        cursor.execute(sql_script)
        conn.commit()
        print("Tables created successfully.")

    except psycopg2.Error as e:
        print(f"Error: {e}")
    finally:
        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# Example usage
create_tables(
    dbname='new_database',  # Name of the database to connect to
    user='your_username',   # Your database username
    password='your_password', # Your database password
    host='localhost',       # Database host
    port='5432'             # Database port
)
