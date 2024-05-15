from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import json
import boto3
import pandas as pd
from io import StringIO
from function_data_validation import get_data, validate_data


class ValidateInputtedData(BaseOperator):

    @apply_defaults
    def __init__(self, bucket_name: str,
                    file_path: str,
                    *args,
                    **kwargs):
        super(ValidateInputtedData, self).__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.file_path = file_path

    def execute(self, context):

        df = get_data(self.bucket_name, self.file_path)

        result_dict = validate_data(df)

        return result_dict
