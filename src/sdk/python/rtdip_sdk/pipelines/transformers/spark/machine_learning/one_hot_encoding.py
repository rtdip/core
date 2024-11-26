# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import functions as F
from ...interfaces import TransformerInterface
from ...._pipeline_utils.models import Libraries, SystemType


class OneHotEncoding(TransformerInterface):
    """
    Performs One-Hot Encoding on a specified column of a PySpark DataFrame.

    Example
    --------
    ```python
    from src.sdk.python.rtdip_sdk.pipelines.data_wranglers.spark.data_quality.one_hot_encoding import OneHotEncoding
    from pyspark.sql import SparkSession


    spark = ... # SparkSession
    df = ... # Get a PySpark DataFrame

    one_hot_encoder = OneHotEncoding(df, "column_name", ["list_of_distinct_values"])
    result_df = one_hot_encoder.encode()
    result_df.show()
    ```

    Parameters:
        df (DataFrame): The PySpark DataFrame to apply encoding on.
        column (str): The name of the column to apply the encoding to.
        values (list, optional): A list of distinct values to encode. If not provided,
                                 the distinct values from the data will be used.
    """

    df: PySparkDataFrame
    column: str
    values: list

    def __init__(self, df: PySparkDataFrame, column: str, values: list = None) -> None:
        self.df = df
        self.column = column
        self.values = values

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_transform_validation(self):
        """
        Validate the input data before transformation.
        - Check if the specified column exists in the DataFrame.
        - If no values are provided, check if the distinct values can be computed.
        - Ensure the DataFrame is not empty.
        """
        if self.df is None or self.df.count() == 0:
            raise ValueError("The DataFrame is empty.")

        if self.column not in self.df.columns:
            raise ValueError(f"Column '{self.column}' does not exist in the DataFrame.")

        if not self.values:
            distinct_values = [
                row[self.column]
                for row in self.df.select(self.column).distinct().collect()
            ]
            if not distinct_values:
                raise ValueError(f"No distinct values found in column '{self.column}'.")
            self.values = distinct_values

    def post_transform_validation(self):
        """
        Validate the result after transformation.
        - Ensure that new columns have been added based on the distinct values.
        - Verify the transformed DataFrame contains the expected number of columns.
        """
        expected_columns = [
            f"{self.column}_{value if value is not None else 'None'}"
            for value in self.values
        ]
        missing_columns = [
            col for col in expected_columns if col not in self.df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing columns in the transformed DataFrame: {missing_columns}"
            )

        if self.df.count() == 0:
            raise ValueError("The transformed DataFrame is empty.")

    def transform(self) -> PySparkDataFrame:
        if not self.values:
            self.values = [
                row[self.column]
                for row in self.df.select(self.column).distinct().collect()
            ]

        for value in self.values:
            self.df = self.df.withColumn(
                f"{self.column}_{value if value is not None else 'None'}",
                F.when(F.col(self.column) == value, 1).otherwise(0),
            )
        return self.df
