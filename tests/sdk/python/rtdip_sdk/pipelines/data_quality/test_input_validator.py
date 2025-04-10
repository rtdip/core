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

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.missing_value_imputation import (
    MissingValueImputation,
)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_input_validator_basic(spark_session: SparkSession):
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

    column_expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
            StructField("Tolerance", FloatType(), True),
        ]
    )

    pyspark_type_schema = {
        "TagName": StringType(),
        "EventTime": TimestampType(),
        "Status": StringType(),
        "Value": float,
    }

    test_data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-01 03:29:21.000", "Good", "1.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 07:32:55.000", "Good", "2.0"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 11:36:29.000", "Good", "3.0"),
    ]

    dirty_data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-01 03:29:21.000", "Good", "abc"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 07:32:55.000", "Good", "rtdip"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 11:36:29.000", "Good", "def"),
    ]

    test_df = spark_session.createDataFrame(test_data, schema=test_schema)
    dirty_df = spark_session.createDataFrame(dirty_data, schema=test_schema)

    test_component = MissingValueImputation(spark_session, test_df)
    dirty_component = MissingValueImputation(spark_session, dirty_df)

    # Check if the column exists
    with pytest.raises(ValueError) as e:
        test_component.validate(column_expected_schema)
    assert "Column 'Tolerance' is missing in the DataFrame." in str(e.value)

    # Check for pyspark Datatypes
    with pytest.raises(TypeError) as e:
        test_component.validate(pyspark_type_schema)
    assert (
        "Expected and actual types must be instances of pyspark.sql.types.DataType."
        in str(e.value)
    )

    # Check for casting failures
    with pytest.raises(ValueError) as e:
        dirty_component.validate(expected_schema)
    assert (
        "Error during casting column 'Value' to FloatType(): Column 'Value' cannot be cast to FloatType()."
        in str(e.value)
    )

    # Check for success
    assert test_component.validate(expected_schema) == True
    assert test_component.df.schema == expected_schema


def test_input_validator_with_null_strings(spark_session: SparkSession):
    # Schema und Testdaten
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

    test_data_with_null_strings = [
        ("A2PS64V0J.:ZUX09R", "2024-01-01 03:29:21.000", "Good", "None"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 07:32:55.000", "Good", "none"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 11:36:29.000", "Good", "Null"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 15:40:00.000", "Good", "null"),
        ("A2PS64V0J.:ZUX09R", "2024-01-01 19:50:00.000", "Good", ""),
    ]

    test_df = spark_session.createDataFrame(
        test_data_with_null_strings, schema=test_schema
    )

    test_component = MissingValueImputation(spark_session, test_df)

    # Validate the DataFrame
    assert test_component.validate(expected_schema) == True
    processed_df = test_component.df

    # Prüfen, ob alle Werte in "Value" None sind
    value_column = processed_df.select("Value").collect()

    for row in value_column:
        assert (
            row["Value"] is None
        ), f"Value {row['Value']} wurde nicht korrekt zu None konvertiert."
