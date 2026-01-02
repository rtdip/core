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
import math

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.cyclical_encoding import (
    CyclicalEncoding,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


@pytest.fixture(scope="session")
def spark():
    spark_session = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark_session
    spark_session.stop()


def test_none_df():
    """None DataFrame raises error"""
    with pytest.raises(ValueError, match="The DataFrame is None."):
        encoder = CyclicalEncoding(None, column="month", period=12)
        encoder.filter_data()


def test_column_not_exists(spark):
    """Non-existent column raises error"""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["month", "value"])

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        encoder = CyclicalEncoding(df, column="nonexistent", period=12)
        encoder.filter_data()


def test_invalid_period(spark):
    """Period <= 0 raises error"""
    df = spark.createDataFrame([(1, 10), (2, 20)], ["month", "value"])

    with pytest.raises(ValueError, match="Period must be positive"):
        encoder = CyclicalEncoding(df, column="month", period=0)
        encoder.filter_data()

    with pytest.raises(ValueError, match="Period must be positive"):
        encoder = CyclicalEncoding(df, column="month", period=-1)
        encoder.filter_data()


def test_month_encoding(spark):
    """Months are encoded correctly (period=12)"""
    df = spark.createDataFrame(
        [(1, 10), (4, 20), (7, 30), (10, 40), (12, 50)], ["month", "value"]
    )

    encoder = CyclicalEncoding(df, column="month", period=12)
    result = encoder.filter_data()

    assert "month_sin" in result.columns
    assert "month_cos" in result.columns

    # December (12) should have sin ≈ 0
    dec_row = result.filter(result["month"] == 12).first()
    assert abs(dec_row["month_sin"] - 0) < 0.01


def test_hour_encoding(spark):
    """Hours are encoded correctly (period=24)"""
    df = spark.createDataFrame(
        [(0, 10), (6, 20), (12, 30), (18, 40), (23, 50)], ["hour", "value"]
    )

    encoder = CyclicalEncoding(df, column="hour", period=24)
    result = encoder.filter_data()

    assert "hour_sin" in result.columns
    assert "hour_cos" in result.columns

    # Hour 0 should have sin=0, cos=1
    h0_row = result.filter(result["hour"] == 0).first()
    assert abs(h0_row["hour_sin"] - 0) < 0.01
    assert abs(h0_row["hour_cos"] - 1) < 0.01

    # Hour 6 should have sin=1, cos≈0
    h6_row = result.filter(result["hour"] == 6).first()
    assert abs(h6_row["hour_sin"] - 1) < 0.01
    assert abs(h6_row["hour_cos"] - 0) < 0.01


def test_weekday_encoding(spark):
    """Weekdays are encoded correctly (period=7)"""
    df = spark.createDataFrame(
        [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)],
        ["weekday", "value"],
    )

    encoder = CyclicalEncoding(df, column="weekday", period=7)
    result = encoder.filter_data()

    assert "weekday_sin" in result.columns
    assert "weekday_cos" in result.columns

    # Monday (0) should have sin ≈ 0
    mon_row = result.filter(result["weekday"] == 0).first()
    assert abs(mon_row["weekday_sin"] - 0) < 0.01


def test_drop_original(spark):
    """Original column is dropped when drop_original=True"""
    df = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["month", "value"])

    encoder = CyclicalEncoding(df, column="month", period=12, drop_original=True)
    result = encoder.filter_data()

    assert "month" not in result.columns
    assert "month_sin" in result.columns
    assert "month_cos" in result.columns
    assert "value" in result.columns


def test_preserves_other_columns(spark):
    """Other columns are preserved"""
    df = spark.createDataFrame(
        [(1, 10, "A"), (2, 20, "B"), (3, 30, "C")], ["month", "value", "category"]
    )

    encoder = CyclicalEncoding(df, column="month", period=12)
    result = encoder.filter_data()

    assert "value" in result.columns
    assert "category" in result.columns
    rows = result.orderBy("month").collect()
    assert rows[0]["value"] == 10
    assert rows[1]["value"] == 20


def test_sin_cos_in_valid_range(spark):
    """Sin and cos values are in range [-1, 1]"""
    df = spark.createDataFrame([(i, i) for i in range(1, 101)], ["value", "id"])

    encoder = CyclicalEncoding(df, column="value", period=100)
    result = encoder.filter_data()

    rows = result.collect()
    for row in rows:
        assert -1 <= row["value_sin"] <= 1
        assert -1 <= row["value_cos"] <= 1


def test_sin_cos_identity(spark):
    """sin² + cos² ≈ 1 for all values"""
    df = spark.createDataFrame([(i,) for i in range(1, 13)], ["month"])

    encoder = CyclicalEncoding(df, column="month", period=12)
    result = encoder.filter_data()

    rows = result.collect()
    for row in rows:
        sum_of_squares = row["month_sin"] ** 2 + row["month_cos"] ** 2
        assert abs(sum_of_squares - 1.0) < 0.01


def test_system_type():
    """Test that system_type returns SystemType.PYSPARK"""
    assert CyclicalEncoding.system_type() == SystemType.PYSPARK


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = CyclicalEncoding.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = CyclicalEncoding.settings()
    assert isinstance(settings, dict)
    assert settings == {}
