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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.data_quality.duplicate_detection import (
    DuplicateDetection,
)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_duplicate_detection(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    duplicate_detection_monitor = DuplicateDetection(df)
    actual_df = duplicate_detection_monitor.filter()

    assert isinstance(actual_df, DataFrame)

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
