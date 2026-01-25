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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.mixed_type_separation import (
    MixedTypeSeparation,
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
        separator = MixedTypeSeparation(None, column="Value")
        separator.filter_data()


def test_column_not_exists(spark):
    df = spark.createDataFrame([("A", "1.0"), ("B", "2.0")], ["TagName", "Value"])

    with pytest.raises(ValueError, match="Column 'NonExistent' does not exist"):
        separator = MixedTypeSeparation(df, column="NonExistent")
        separator.filter_data()


def test_all_numeric_values(spark):
    df = spark.createDataFrame(
        [("A", "1.0"), ("B", "2.5"), ("C", "3.14")], ["TagName", "Value"]
    )

    separator = MixedTypeSeparation(df, column="Value")
    result_df = separator.filter_data()

    assert "Value_str" in result_df.columns

    rows = result_df.orderBy("TagName").collect()
    assert all(row["Value_str"] == "NaN" for row in rows)
    assert rows[0]["Value"] == 1.0
    assert rows[1]["Value"] == 2.5
    assert rows[2]["Value"] == 3.14


def test_all_string_values(spark):
    df = spark.createDataFrame(
        [("A", "Bad"), ("B", "Error"), ("C", "N/A")], ["TagName", "Value"]
    )

    separator = MixedTypeSeparation(df, column="Value", placeholder=-1.0)
    result_df = separator.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[0]["Value_str"] == "Bad"
    assert rows[1]["Value_str"] == "Error"
    assert rows[2]["Value_str"] == "N/A"
    assert all(row["Value"] == -1.0 for row in rows)


def test_mixed_values(spark):
    df = spark.createDataFrame(
        [("A", "3.14"), ("B", "Bad"), ("C", "100"), ("D", "Error")],
        ["TagName", "Value"],
    )

    separator = MixedTypeSeparation(df, column="Value", placeholder=-1.0)
    result_df = separator.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[0]["Value"] == 3.14
    assert rows[0]["Value_str"] == "NaN"
    assert rows[1]["Value"] == -1.0
    assert rows[1]["Value_str"] == "Bad"
    assert rows[2]["Value"] == 100.0
    assert rows[2]["Value_str"] == "NaN"
    assert rows[3]["Value"] == -1.0
    assert rows[3]["Value_str"] == "Error"


def test_numeric_strings(spark):
    df = spark.createDataFrame(
        [("A", "3.14"), ("B", "1e-5"), ("C", "-100"), ("D", "Bad")],
        ["TagName", "Value"],
    )

    separator = MixedTypeSeparation(df, column="Value", placeholder=-1.0)
    result_df = separator.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[0]["Value"] == 3.14
    assert rows[0]["Value_str"] == "NaN"
    assert abs(rows[1]["Value"] - 1e-5) < 1e-10
    assert rows[1]["Value_str"] == "NaN"
    assert rows[2]["Value"] == -100.0
    assert rows[2]["Value_str"] == "NaN"
    assert rows[3]["Value"] == -1.0
    assert rows[3]["Value_str"] == "Bad"


def test_custom_placeholder(spark):
    df = spark.createDataFrame([("A", "10.0"), ("B", "Error")], ["TagName", "Value"])

    separator = MixedTypeSeparation(df, column="Value", placeholder=-999.0)
    result_df = separator.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[1]["Value"] == -999.0


def test_custom_string_fill(spark):
    df = spark.createDataFrame([("A", "10.0"), ("B", "Error")], ["TagName", "Value"])

    separator = MixedTypeSeparation(df, column="Value", string_fill="NUMERIC")
    result_df = separator.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[0]["Value_str"] == "NUMERIC"
    assert rows[1]["Value_str"] == "Error"


def test_custom_suffix(spark):
    df = spark.createDataFrame([("A", "10.0"), ("B", "Error")], ["TagName", "Value"])

    separator = MixedTypeSeparation(df, column="Value", suffix="_text")
    result_df = separator.filter_data()

    assert "Value_text" in result_df.columns
    assert "Value_str" not in result_df.columns


def test_preserves_other_columns(spark):
    df = spark.createDataFrame(
        [
            ("Tag_A", "2024-01-02 20:03:46", "Good", "1.0"),
            ("Tag_B", "2024-01-02 16:00:12", "Bad", "Error"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    separator = MixedTypeSeparation(df, column="Value")
    result_df = separator.filter_data()

    assert "TagName" in result_df.columns
    assert "EventTime" in result_df.columns
    assert "Status" in result_df.columns
    assert "Value" in result_df.columns
    assert "Value_str" in result_df.columns


def test_null_values(spark):
    df = spark.createDataFrame(
        [("A", "1.0"), ("B", None), ("C", "Bad")], ["TagName", "Value"]
    )

    separator = MixedTypeSeparation(df, column="Value", placeholder=-1.0)
    result_df = separator.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[0]["Value"] == 1.0
    assert rows[1]["Value"] is None or rows[1]["Value_str"] == "NaN"
    assert rows[2]["Value"] == -1.0
    assert rows[2]["Value_str"] == "Bad"


def test_special_string_values(spark):
    df = spark.createDataFrame(
        [("A", "1.0"), ("B", ""), ("C", "  ")], ["TagName", "Value"]
    )

    separator = MixedTypeSeparation(df, column="Value", placeholder=-1.0)
    result_df = separator.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[0]["Value"] == 1.0
    assert rows[1]["Value"] == -1.0
    assert rows[1]["Value_str"] == ""
    assert rows[2]["Value"] == -1.0
    assert rows[2]["Value_str"] == "  "


def test_integer_placeholder(spark):
    df = spark.createDataFrame([("A", "10.0"), ("B", "Error")], ["TagName", "Value"])

    separator = MixedTypeSeparation(df, column="Value", placeholder=-1)
    result_df = separator.filter_data()

    rows = result_df.orderBy("TagName").collect()
    assert rows[1]["Value"] == -1.0


def test_system_type():
    assert MixedTypeSeparation.system_type() == SystemType.PYSPARK


def test_libraries():
    libraries = MixedTypeSeparation.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    settings = MixedTypeSeparation.settings()
    assert isinstance(settings, dict)
    assert settings == {}
