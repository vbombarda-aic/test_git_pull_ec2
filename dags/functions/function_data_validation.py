import pandas as pd
# import great_expectations as ge
import boto3
from io import StringIO

# bucket_name = "argo-data-lake"
# file_path = "data_example.csv"

def get_data(bucket_name, file_path):
    # Sets connection to bucket
    client = boto3.client("s3")

    # Extracts the given data
    obj = client.get_object(Bucket=bucket_name, Key=file_path)
    csv_content = obj['Body'].read().decode('utf-8')

    # Serializes the data
    csv_string_io = StringIO(csv_content)

    # Creates a Dataframe
    df = pd.read_csv(csv_string_io)

    return df

def validate_data(data):

    # Structure Columns
    UserID = "RespondentID"
    DateTime = "Timestamp"
    ContentID = "Content"
    Survey = "Survey"
    columnsList = [UserID, DateTime, ContentID, Survey]

    # Control lists for the final response
    foundNullValues = []
    duplicatedEntries = []
    missingColumns = list(set(columnsList) - set(list(data.columns)))

    # Create DataAsset
    data_asset = ge.dataset.PandasDataset(data)


    # Expectations
    #### Checks for existing columns
    expectations_exists = [
        
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": UserID}
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": DateTime}
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": ContentID}
        },
        {
            "expectation_type": "expect_column_to_exist",
            "kwargs": {"column": Survey}
        }
    ]

    #### Checks for null values in columns
    expectations_null =  [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": column}
        } for column in columnsList if column not in missingColumns
    ]

    #### Checks for duplicated values in column(s)
    expectations_duplicated =  [
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": column}
        } for column in columnsList if column == UserID and column not in missingColumns
    ]


    expectations = expectations_exists + expectations_null + expectations_duplicated

    # Apply expectations to the data set
    for expectation in expectations:
        expectation_type = expectation["expectation_type"]
        kwargs = expectation.get("kwargs", {})
        meta = expectation.get("meta", {})
        
        
        expectation_method = getattr(data_asset, expectation_type)
        expectation_method(**kwargs, meta=meta)


    #### Validation ####
    validation_result = data_asset.validate()

    results = validation_result["results"]

    # Result for Fixed Columns
    isOK = True

    for i in range(len(results)):
            result = results[i]
            success = result["success"]
            expectation_config = result["expectation_config"]
            expectation_type = expectation_config["expectation_type"]
            column = expectation_config["kwargs"]["column"]
            if not success:
                isOK = False
                # if expectation_type == "expect_column_to_exist":
                #      missingColumns.append(column)
                if expectation_type == "expect_column_values_to_not_be_null":
                    foundNullValues.append(column)
                elif expectation_type == "expect_column_values_to_be_unique":
                    duplicatedEntries.append(column)

    if isOK:
        print("#########################")
        print("No errors found!")
        print("#########################")
    else:
        print("#########################")
        if len(foundNullValues):
            print("Found null values in the column(s): ", foundNullValues)
        print("#########################")
        if len(missingColumns):
            print("The following column(s) were not found: ", missingColumns)
        print("#########################")
        if len(duplicatedEntries):
            print("Found duplicated values in the column(s): ", duplicatedEntries)
        print("#########################")
    
    
    return {"null": foundNullValues, "missing": missingColumns, "duplicated": duplicatedEntries}
