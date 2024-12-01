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
from pyspark.sql.dataframe import DataFrame

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
    result_df = duplicate_detection.filter()
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
    result_df = duplicate_detection.filter()
    result_df.show()

    assert (
        result_df.count() == expected_df.count()
    ), "Row count does not match expected result"
    assert sorted(result_df.collect()) == sorted(
        expected_df.collect()
    ), "Data does not match expected result"
