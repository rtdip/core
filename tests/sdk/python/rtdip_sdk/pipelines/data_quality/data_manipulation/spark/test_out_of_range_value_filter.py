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
from pyspark.sql import SparkSession
import os


from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.out_of_range_value_filter import (
    OutOfRangeValueFilter,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("DeleteOutOfRangeValuesTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def test_data(spark):
    data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "1"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "2"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "3"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "4"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "5"),
        ("Tag2", "2024-01-02 03:49:45.000", "Good", "1"),
        ("Tag2", "2024-01-02 07:53:11.000", "Good", "2"),
        ("Tag2", "2024-01-02 11:56:42.000", "Good", "3"),
        ("Tag2", "2024-01-02 16:00:12.000", "Good", "4"),
        ("Tag2", "2024-01-02 20:03:46.000", "Good", "5"),
    ]
    return spark.createDataFrame(data, ["TagName", "EventTime", "Status", "Value"])


def test_basic(spark, test_data):
    tag_ranges = {
        "A2PS64V0J.:ZUX09R": {"min": 2, "max": 4, "inclusive_bounds": True},
        "Tag2": {"min": 1, "max": 5, "inclusive_bounds": False},
    }
    manipulator = OutOfRangeValueFilter(test_data, tag_ranges)

    rows_to_remove = [
        {
            "TagName": "A2PS64V0J.:ZUX09R",
            "EventTime": "2024-01-02 07:53:11.000",
            "Status": "Good",
            "Value": "2",
        },
        {
            "TagName": "Tag2",
            "EventTime": "2024-01-02 11:56:42.000",
            "Status": "Good",
            "Value": "3",
        },
    ]
    rows_to_remove_df = spark.createDataFrame(rows_to_remove)
    expected = test_data.subtract(rows_to_remove_df)

    result = manipulator.filter_data()

    assert sorted(result.collect()) == sorted(expected.collect())


def test_large_dataset(spark):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    df = spark.read.option("header", "true").csv(file_path)
    assert df.count() > 0, "Dataframe was not loaded correct"

    tag_ranges = {
        "value_range": {"min": 2, "max": 4, "inclusive_bounds": True},
    }
    manipulator = OutOfRangeValueFilter(df, tag_ranges)

    rows_to_remove = [
        {
            "TagName": "value_range",
            "EventTime": "2024-01-02 03:49:45",
            "Status": "Good",
            "Value": "1.0",
        },
        {
            "TagName": "value_range",
            "EventTime": "2024-01-02 20:03:46",
            "Status": "Good",
            "Value": "5.0",
        },
    ]
    rows_to_remove_df = spark.createDataFrame(rows_to_remove)
    expected = df.subtract(rows_to_remove_df)

    result = manipulator.filter_data()

    assert sorted(result.collect()) == sorted(expected.collect())
