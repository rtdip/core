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
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.machine_learning.one_hot_encoding import (
    OneHotEncoding,
)

# Define the schema outside the test functions
SCHEMA = StructType(
    [
        StructField("TagName", StringType(), True),
        StructField("EventTime", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("Value", FloatType(), True),
    ]
)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_empty_df(spark_session):
    """Empty DataFrame"""
    empty_data = []
    empty_df = spark_session.createDataFrame(empty_data, SCHEMA)
    encoder = OneHotEncoding(empty_df, "TagName")
    result_df = encoder.transform()

    assert (
        result_df.count() == 0
    ), "Expected no rows in the result DataFrame for empty input."
    assert result_df.columns == [
        "TagName",
        "EventTime",
        "Status",
        "Value",
    ], "Expected no new columns for empty DataFrame."


def test_single_unique_value(spark_session):
    """Single Unique Value"""
    data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46", "Good", 0.34),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12", "Good", 0.15),
    ]
    df = spark_session.createDataFrame(data, SCHEMA)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.transform()

    expected_columns = [
        "TagName",
        "EventTime",
        "Status",
        "Value",
        "TagName_A2PS64V0J.:ZUX09R",
    ]
    assert (
        result_df.columns == expected_columns
    ), "Columns do not match for single unique value."
    for row in result_df.collect():
        assert (
            row["TagName_A2PS64V0J.:ZUX09R"] == 1
        ), "Expected 1 for the one-hot encoded column."


def test_null_values(spark_session):
    """Column with Null Values"""
    data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46", "Good", 0.34),
        (None, "2024-01-02 16:00:12", "Good", 0.15),
    ]
    df = spark_session.createDataFrame(data, SCHEMA)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.transform()

    expected_columns = [
        "TagName",
        "EventTime",
        "Status",
        "Value",
        "TagName_A2PS64V0J.:ZUX09R",
        "TagName_None",
    ]
    assert (
        result_df.columns == expected_columns
    ), f"Columns do not match for null value case. Expected {expected_columns}, but got {result_df.columns}"
    for row in result_df.collect():
        if row["TagName"] == "A2PS64V0J.:ZUX09R":
            assert (
                row["TagName_A2PS64V0J.:ZUX09R"] == 1
            ), "Expected 1 for valid TagName."
            assert (
                row["TagName_None"] == 0
            ), "Expected 0 for TagName_None for valid TagName."
        elif row["TagName"] is None:
            assert (
                row["TagName_A2PS64V0J.:ZUX09R"] == 0
            ), "Expected 0 for TagName_A2PS64V0J.:ZUX09R for None TagName."
            assert (
                row["TagName_None"] == 0
            ), "Expected 0 for TagName_None for None TagName."


def test_large_unique_values(spark_session):
    """Large Number of Unique Values"""
    data = [
        (f"Tag_{i}", f"2024-01-02 20:03:{i:02d}", "Good", i * 1.0) for i in range(1000)
    ]
    df = spark_session.createDataFrame(data, SCHEMA)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.transform()

    assert (
        len(result_df.columns) == len(SCHEMA.fields) + 1000
    ), "Expected 1000 additional columns for one-hot encoding."


def test_special_characters(spark_session):
    """Special Characters in Column Values"""
    data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46", "Good", 0.34),
        ("@Special#Tag!", "2024-01-02 16:00:12", "Good", 0.15),
    ]
    df = spark_session.createDataFrame(data, SCHEMA)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.transform()

    expected_columns = [
        "TagName",
        "EventTime",
        "Status",
        "Value",
        "TagName_A2PS64V0J.:ZUX09R",
        "TagName_@Special#Tag!",
    ]
    assert (
        result_df.columns == expected_columns
    ), "Columns do not match for special characters."
    for row in result_df.collect():
        for tag in ["A2PS64V0J.:ZUX09R", "@Special#Tag!"]:
            expected_value = 1 if row["TagName"] == tag else 0
            column_name = f"TagName_{tag}"
            assert (
                row[column_name] == expected_value
            ), f"Expected {expected_value} for {column_name}."


def test_distinct_value(spark_session):
    """Dataset with Multiple TagName Values"""

    data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46", "Good", 0.3400000035762787),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12", "Good", 0.15000000596046448),
        (
            "-4O7LSSAM_3EA02:2GT7E02I_R_MP",
            "2024-01-02 20:09:58",
            "Good",
            7107.82080078125,
        ),
        ("_LT2EPL-9PM0.OROTENV3:", "2024-01-02 12:27:10", "Good", 19407.0),
        ("1N325T3MTOR-P0L29:9.T0", "2024-01-02 23:41:10", "Good", 19376.0),
    ]

    df = spark_session.createDataFrame(data, SCHEMA)

    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.transform()

    result = result_df.collect()

    expected_columns = df.columns + [
        f"TagName_{row['TagName']}" for row in df.select("TagName").distinct().collect()
    ]

    assert set(result_df.columns) == set(expected_columns)

    tag_names = df.select("TagName").distinct().collect()
    for row in result:
        tag_name = row["TagName"]
        for tag in tag_names:
            column_name = f"TagName_{tag['TagName']}"
            if tag["TagName"] == tag_name:
                assert row[column_name] == 1.0
            else:
                assert row[column_name] == 0.0
