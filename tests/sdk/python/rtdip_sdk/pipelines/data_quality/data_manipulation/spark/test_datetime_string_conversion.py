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
from pyspark.sql.types import TimestampType

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.datetime_string_conversion import (
    DatetimeStringConversion,
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
        converter = DatetimeStringConversion(None, column="EventTime")
        converter.filter_data()


def test_column_not_exists(spark):
    df = spark.createDataFrame([("A", "2024-01-01")], ["sensor_id", "timestamp"])

    with pytest.raises(ValueError, match="Column 'EventTime' does not exist"):
        converter = DatetimeStringConversion(df, column="EventTime")
        converter.filter_data()


def test_empty_formats(spark):
    df = spark.createDataFrame(
        [("A", "2024-01-01 10:00:00")], ["sensor_id", "EventTime"]
    )

    with pytest.raises(
        ValueError, match="At least one datetime format must be provided"
    ):
        converter = DatetimeStringConversion(df, column="EventTime", formats=[])
        converter.filter_data()


def test_standard_format_without_microseconds(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02 20:03:46"),
            ("B", "2024-01-02 16:00:12"),
        ],
        ["sensor_id", "EventTime"],
    )

    converter = DatetimeStringConversion(df, column="EventTime")
    result_df = converter.filter_data()

    assert "EventTime_DT" in result_df.columns
    assert result_df.schema["EventTime_DT"].dataType == TimestampType()

    rows = result_df.collect()
    assert all(row["EventTime_DT"] is not None for row in rows)


def test_standard_format_with_milliseconds(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02 20:03:46.123"),
            ("B", "2024-01-02 16:00:12.456"),
        ],
        ["sensor_id", "EventTime"],
    )

    converter = DatetimeStringConversion(df, column="EventTime")
    result_df = converter.filter_data()

    rows = result_df.collect()
    assert all(row["EventTime_DT"] is not None for row in rows)


def test_mixed_formats(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02 20:03:46.000"),
            ("B", "2024-01-02 16:00:12"),
            ("C", "2024-01-02T11:56:42"),
        ],
        ["sensor_id", "EventTime"],
    )

    converter = DatetimeStringConversion(df, column="EventTime")
    result_df = converter.filter_data()

    rows = result_df.collect()
    assert all(row["EventTime_DT"] is not None for row in rows)


def test_custom_output_column(spark):
    df = spark.createDataFrame(
        [("A", "2024-01-02 20:03:46")], ["sensor_id", "EventTime"]
    )

    converter = DatetimeStringConversion(
        df, column="EventTime", output_column="Timestamp"
    )
    result_df = converter.filter_data()

    assert "Timestamp" in result_df.columns
    assert "EventTime_DT" not in result_df.columns


def test_keep_original_true(spark):
    df = spark.createDataFrame(
        [("A", "2024-01-02 20:03:46")], ["sensor_id", "EventTime"]
    )

    converter = DatetimeStringConversion(df, column="EventTime", keep_original=True)
    result_df = converter.filter_data()

    assert "EventTime" in result_df.columns
    assert "EventTime_DT" in result_df.columns


def test_keep_original_false(spark):
    df = spark.createDataFrame(
        [("A", "2024-01-02 20:03:46")], ["sensor_id", "EventTime"]
    )

    converter = DatetimeStringConversion(df, column="EventTime", keep_original=False)
    result_df = converter.filter_data()

    assert "EventTime" not in result_df.columns
    assert "EventTime_DT" in result_df.columns


def test_invalid_datetime_string(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02 20:03:46"),
            ("B", "invalid_datetime"),
            ("C", "not_a_date"),
        ],
        ["sensor_id", "EventTime"],
    )

    converter = DatetimeStringConversion(df, column="EventTime")
    result_df = converter.filter_data()

    rows = result_df.orderBy("sensor_id").collect()
    assert rows[0]["EventTime_DT"] is not None
    assert rows[1]["EventTime_DT"] is None
    assert rows[2]["EventTime_DT"] is None


def test_iso_format(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02T20:03:46"),
            ("B", "2024-01-02T16:00:12.123"),
        ],
        ["sensor_id", "EventTime"],
    )

    converter = DatetimeStringConversion(df, column="EventTime")
    result_df = converter.filter_data()

    rows = result_df.collect()
    assert all(row["EventTime_DT"] is not None for row in rows)


def test_custom_formats(spark):
    df = spark.createDataFrame(
        [
            ("A", "02/01/2024 20:03:46"),
            ("B", "03/01/2024 16:00:12"),
        ],
        ["sensor_id", "EventTime"],
    )

    converter = DatetimeStringConversion(
        df, column="EventTime", formats=["dd/MM/yyyy HH:mm:ss"]
    )
    result_df = converter.filter_data()

    rows = result_df.collect()
    assert all(row["EventTime_DT"] is not None for row in rows)


def test_preserves_other_columns(spark):
    df = spark.createDataFrame(
        [
            ("Tag_A", "2024-01-02 20:03:46", 1.0),
            ("Tag_B", "2024-01-02 16:00:12", 2.0),
        ],
        ["TagName", "EventTime", "Value"],
    )

    converter = DatetimeStringConversion(df, column="EventTime")
    result_df = converter.filter_data()

    assert "TagName" in result_df.columns
    assert "Value" in result_df.columns

    rows = result_df.orderBy("Value").collect()
    assert rows[0]["TagName"] == "Tag_A"
    assert rows[1]["TagName"] == "Tag_B"


def test_null_values(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02 20:03:46"),
            ("B", None),
            ("C", "2024-01-02 16:00:12"),
        ],
        ["sensor_id", "EventTime"],
    )

    converter = DatetimeStringConversion(df, column="EventTime")
    result_df = converter.filter_data()

    rows = result_df.orderBy("sensor_id").collect()
    assert rows[0]["EventTime_DT"] is not None
    assert rows[1]["EventTime_DT"] is None
    assert rows[2]["EventTime_DT"] is not None


def test_trailing_zeros(spark):
    df = spark.createDataFrame(
        [
            ("A", "2024-01-02 20:03:46.000"),
            ("B", "2024-01-02 16:00:12.000"),
        ],
        ["sensor_id", "EventTime"],
    )

    converter = DatetimeStringConversion(df, column="EventTime")
    result_df = converter.filter_data()

    rows = result_df.collect()
    assert all(row["EventTime_DT"] is not None for row in rows)


def test_system_type():
    assert DatetimeStringConversion.system_type() == SystemType.PYSPARK


def test_libraries():
    libraries = DatetimeStringConversion.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    settings = DatetimeStringConversion.settings()
    assert isinstance(settings, dict)
    assert settings == {}
