# Copyright 2025 RTDIP
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
import os
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.duplicate_detection import (
    DuplicateDetection,
)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


@pytest.fixture
def test_data(spark_session):
    data = [
        ("key1", "time1", "value1"),
        ("key2", "time2", "value2"),
        ("key2", "time3", "value2"),
        ("key1", "time1", "value3"),
        ("key4", "time4", "value4"),
        ("key5", "time4", "value5"),
    ]
    columns = ["TagName", "EventTime", "Value"]
    return spark_session.createDataFrame(data, columns)


def test_duplicate_detection_two_columns(spark_session, test_data):
    expected_data = [
        ("key1", "time1", "value1"),
        ("key2", "time2", "value2"),
        ("key2", "time3", "value2"),
        ("key4", "time4", "value4"),
        ("key5", "time4", "value5"),
    ]
    columns = ["TagName", "EventTime", "Value"]
    expected_df = spark_session.createDataFrame(expected_data, columns)

    duplicate_detection = DuplicateDetection(
        test_data, primary_key_columns=["TagName", "EventTime"]
    )
    result_df = duplicate_detection.filter_data()
    result_df.show()

    assert (
        result_df.count() == expected_df.count()
    ), "Row count does not match expected result"
    assert sorted(result_df.collect()) == sorted(
        expected_df.collect()
    ), "Data does not match expected result"


def test_duplicate_detection_one_column(spark_session, test_data):
    expected_data = [
        ("key1", "time1", "value1"),
        ("key2", "time2", "value2"),
        ("key4", "time4", "value4"),
        ("key5", "time4", "value5"),
    ]
    columns = ["TagName", "EventTime", "Value"]
    expected_df = spark_session.createDataFrame(expected_data, columns)

    duplicate_detection = DuplicateDetection(test_data, primary_key_columns=["TagName"])
    result_df = duplicate_detection.filter_data()
    result_df.show()

    assert (
        result_df.count() == expected_df.count()
    ), "Row count does not match expected result"
    assert sorted(result_df.collect()) == sorted(
        expected_df.collect()
    ), "Data does not match expected result"


def test_duplicate_detection_large_data_set(spark_session: SparkSession):
    test_path = os.path.dirname(__file__)
    data_path = os.path.join(test_path, "../../test_data.csv")

    actual_df = spark_session.read.option("header", "true").csv(data_path)

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    duplicate_detection_component = DuplicateDetection(
        actual_df, primary_key_columns=["TagName", "EventTime"]
    )
    result_df = DataFrame

    try:
        if duplicate_detection_component.validate(expected_schema):
            result_df = duplicate_detection_component.filter_data()
    except Exception as e:
        print(repr(e))

    assert isinstance(actual_df, DataFrame)

    assert result_df.schema == expected_schema
    assert result_df.count() < actual_df.count()
    assert result_df.count() == (actual_df.count() - 4)


def test_duplicate_detection_wrong_datatype(spark_session: SparkSession):

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    test_df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "1.0"),
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "2.0"),
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "3.0"),
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "4.0"),
            ("A2PS64V0J.:ZUX09R", "invalid_data_type", "Good", "5.0"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    duplicate_detection_component = DuplicateDetection(
        test_df, primary_key_columns=["TagName", "EventTime"]
    )

    with pytest.raises(ValueError) as exc_info:
        duplicate_detection_component.validate(expected_schema)

    assert (
        "Error during casting column 'EventTime' to TimestampType(): Column 'EventTime' cannot be cast to TimestampType()."
        in str(exc_info.value)
    )
