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

from pyspark.sql.types import DataType, StructType
from pyspark.sql import functions as F
from ..interfaces import PipelineComponentBaseInterface
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class InputValidator(PipelineComponentBaseInterface):
    """
    Validates the PySpark DataFrame of the respective child class instance against a schema dictionary or pyspark
    StructType. Checks for column availability and column data types. If data types differ, it tries to cast the
    column into the expected data type. Casts "None", "none", "Null", "null" and "" to None. Raises Errors if some step fails.

    Example:
    --------
    import pytest
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
    from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.missing_value_imputation import (
        MissingValueImputation,
    )

    @pytest.fixture(scope="session")
    def spark_session():
        return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

    spark = spark_session()

    test_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", StringType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", StringType(), True),
        ]
    )
    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    test_data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-01 03:29:21.000", "Good", "1.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 07:32:55.000", "Good", "2.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 11:36:29.000", "Good", "3.0"),
    ]

    test_df = spark_session.createDataFrame(test_data, schema=test_schema)
    test_component = MissingValueImputation(spark_session, test_df)

    print(test_component.validate(expected_schema)) # True

    ```

    Parameters:
        schema_dict: dict or pyspark StructType
            A dictionary where keys are column names, and values are expected PySpark data types.
            Example: {"column1": StringType(), "column2": IntegerType()}

    Returns:
        True: if data is valid
        Raises Error else

    Raises:
        ValueError: If a column is missing or has a mismatched pyspark data type.
        TypeError: If a column does not hold or specify a pyspark data type.
    """

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

    def validate(self, schema_dict):
        """
        Used by child data quality utility classes to validate the input data.
        """
        dataframe = getattr(self, "df", None)

        if isinstance(schema_dict, StructType):
            schema_dict = {field.name: field.dataType for field in schema_dict.fields}

        dataframe_schema = {
            field.name: field.dataType for field in dataframe.schema.fields
        }

        for column, expected_type in schema_dict.items():
            if column in dataframe.columns:
                dataframe = dataframe.withColumn(
                    column,
                    F.when(
                        F.col(column).isin("None", "none", "null", "Null", ""), None
                    ).otherwise(F.col(column)),
                )

        for column, expected_type in schema_dict.items():
            # Check if the column exists
            if column not in dataframe_schema:
                raise ValueError(f"Column '{column}' is missing in the DataFrame.")

            # Check if both types are of a pyspark data type
            actual_type = dataframe_schema[column]
            if not isinstance(actual_type, DataType) or not isinstance(
                expected_type, DataType
            ):
                raise TypeError(
                    "Expected and actual types must be instances of pyspark.sql.types.DataType."
                )

            # Check if actual type is expected type, try to cast else
            if not isinstance(actual_type, type(expected_type)):
                try:
                    original_null_count = dataframe.filter(
                        F.col(column).isNull()
                    ).count()
                    casted_column = dataframe.withColumn(
                        column, F.col(column).cast(expected_type)
                    )
                    new_null_count = casted_column.filter(
                        F.col(column).isNull()
                    ).count()

                    if new_null_count > original_null_count:
                        raise ValueError(
                            f"Column '{column}' cannot be cast to {expected_type}."
                        )
                    dataframe = casted_column
                except Exception as e:
                    raise ValueError(
                        f"Error during casting column '{column}' to {expected_type}: {str(e)}"
                    )

        self.df = dataframe
        return True
