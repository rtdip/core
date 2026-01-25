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
from datetime import datetime

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.chronological_sort import (
    ChronologicalSort,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = (
        SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_none_df():
    with pytest.raises(ValueError, match="The DataFrame is None."):
        sorter = ChronologicalSort(None, datetime_column="timestamp")
        sorter.filter_data()


def test_column_not_exists(spark):
    df = spark.createDataFrame(
        [("A", "2024-01-01", 10)], ["sensor_id", "timestamp", "value"]
    )

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        sorter = ChronologicalSort(df, datetime_column="nonexistent")
        sorter.filter_data()


def test_group_column_not_exists(spark):
    df = spark.createDataFrame(
        [("A", "2024-01-01", 10)], ["sensor_id", "timestamp", "value"]
    )

    with pytest.raises(ValueError, match="Group column 'region' does not exist"):
        sorter = ChronologicalSort(
            df, datetime_column="timestamp", group_columns=["region"]
        )
        sorter.filter_data()


def test_basic_sort_ascending(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-03", 30),
            ("B", "2024-01-01", 10),
            ("C", "2024-01-02", 20),
        ],
        ["sensor_id", "timestamp", "value"],
    )

    sorter = ChronologicalSort(df, datetime_column="timestamp", ascending=True)
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["value"] for row in rows] == [10, 20, 30]


def test_basic_sort_descending(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-03", 30),
            ("B", "2024-01-01", 10),
            ("C", "2024-01-02", 20),
        ],
        ["sensor_id", "timestamp", "value"],
    )

    sorter = ChronologicalSort(df, datetime_column="timestamp", ascending=False)
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["value"] for row in rows] == [30, 20, 10]


def test_sort_with_groups(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02", 20),
            ("A", "2024-01-01", 10),
            ("B", "2024-01-02", 200),
            ("B", "2024-01-01", 100),
        ],
        ["sensor_id", "timestamp", "value"],
    )

    sorter = ChronologicalSort(
        df, datetime_column="timestamp", group_columns=["sensor_id"]
    )
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["sensor_id"] for row in rows] == ["A", "A", "B", "B"]
    assert [row["value"] for row in rows] == [10, 20, 100, 200]


def test_null_values_last(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02", 20),
            ("B", None, 0),
            ("C", "2024-01-01", 10),
        ],
        ["sensor_id", "timestamp", "value"],
    )

    sorter = ChronologicalSort(df, datetime_column="timestamp", nulls_last=True)
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["value"] for row in rows] == [10, 20, 0]
    assert rows[-1]["timestamp"] is None


def test_null_values_first(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02", 20),
            ("B", None, 0),
            ("C", "2024-01-01", 10),
        ],
        ["sensor_id", "timestamp", "value"],
    )

    sorter = ChronologicalSort(df, datetime_column="timestamp", nulls_last=False)
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["value"] for row in rows] == [0, 10, 20]
    assert rows[0]["timestamp"] is None


def test_already_sorted(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-01", 10),
            ("B", "2024-01-02", 20),
            ("C", "2024-01-03", 30),
        ],
        ["sensor_id", "timestamp", "value"],
    )

    sorter = ChronologicalSort(df, datetime_column="timestamp")
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["value"] for row in rows] == [10, 20, 30]


def test_preserves_other_columns(spark):
    df = spark.createDataFrame(
        [
            ("C", "2024-01-03", "Good", 30),
            ("A", "2024-01-01", "Bad", 10),
            ("B", "2024-01-02", "Good", 20),
        ],
        ["TagName", "timestamp", "Status", "Value"],
    )

    sorter = ChronologicalSort(df, datetime_column="timestamp")
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["TagName"] for row in rows] == ["A", "B", "C"]
    assert [row["Status"] for row in rows] == ["Bad", "Good", "Good"]
    assert [row["Value"] for row in rows] == [10, 20, 30]


def test_with_timestamp_type(spark):
    df = spark.createDataFrame(
        [
            ("A", datetime(2024, 1, 3, 10, 0, 0), 30),
            ("B", datetime(2024, 1, 1, 10, 0, 0), 10),
            ("C", datetime(2024, 1, 2, 10, 0, 0), 20),
        ],
        ["sensor_id", "timestamp", "value"],
    )

    sorter = ChronologicalSort(df, datetime_column="timestamp")
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["value"] for row in rows] == [10, 20, 30]


def test_multiple_group_columns(spark):
    df = spark.createDataFrame(
        [
            ("East", "A", "2024-01-02", 20),
            ("East", "A", "2024-01-01", 10),
            ("West", "A", "2024-01-02", 200),
            ("West", "A", "2024-01-01", 100),
        ],
        ["region", "sensor_id", "timestamp", "value"],
    )

    sorter = ChronologicalSort(
        df, datetime_column="timestamp", group_columns=["region", "sensor_id"]
    )
    result_df = sorter.filter_data()

    rows = result_df.collect()
    assert [row["region"] for row in rows] == ["East", "East", "West", "West"]
    assert [row["value"] for row in rows] == [10, 20, 100, 200]


def test_system_type():
    assert ChronologicalSort.system_type() == SystemType.PYSPARK


def test_libraries():
    libraries = ChronologicalSort.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    settings = ChronologicalSort.settings()
    assert isinstance(settings, dict)
    assert settings == {}
