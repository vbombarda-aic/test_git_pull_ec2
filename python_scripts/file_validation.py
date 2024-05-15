import pandas as pd
import great_expectations as ge

def validate_data(data_path):
    # Load the data
    df = pd.read_spss(data_path)

    expected_answers_1 =['Agree',
                         'Strongly agree',
                         'Neither agree nor disagree',
                         'Somewhat agree',
                         'Strongly disagree',
                         'Somewhat disagree',
                         'Disagree']

    # Create a validation suite
    expectation_suite_basic = ge.dataset.PandasDataset(df)

    #### Initial basic integrity => ID + Date + Content

    # Check the integrity of certain Fundamental Columns
    expectation_suite_basic.expect_column_values_to_not_be_null("uuid")       # ID
    expectation_suite_basic.expect_column_values_to_be_unique("uuid")
    expectation_suite_basic.expect_column_values_to_not_be_null("date")       # DATE
    expectation_suite_basic.expect_column_values_to_not_be_null("QPASSWORD")  # CONTENT


    #### Integrity towards the expected
    questions_1 = [col for col in df.columns if col.startswith('QAGREEMENT') or col.startswith('QHISTORICAL') or col.startswith("QEXPERIENCE")]
    df_questions = df[questions_1]
    expectation_suite_agreement = ge.dataset.PandasDataset(df_questions)

    for column in df_questions.columns:
        expectation_suite_agreement.expect_column_values_to_be_in_set(column, expected_answers_1)


    # Validate the data
    validation_results_1 = expectation_suite_basic.validate()

    print("############### Integrity ###############")
    # Print validation results
    for result in validation_results_1["results"]:
        print(result["expectation_config"]["expectation_type"], result["success"], sep=" ")
        print()
    print("#########################################")

    validation_results_2 = expectation_suite_agreement.validate()

    print("################# Values #################")
    # Print validation results
    for result in validation_results_2["results"]:
        # print(result)
        print(result["expectation_config"]["kwargs"]["column"], result["success"], result["result"]["missing_percent"], sep=" ")
        print()
    print("#########################################")

# Run the validation
validate_data("/mnt/c/users/victo/Downloads/Phase 4 Raw Data.sav")
