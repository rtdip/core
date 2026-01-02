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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.datetime_features import (
    DatetimeFeatures,
    AVAILABLE_FEATURES,
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
        extractor = DatetimeFeatures(None, "timestamp")
        extractor.filter_data()


def test_column_not_exists(spark):
    """Non-existent column raises error"""
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-01-02", 2)], ["timestamp", "value"]
    )

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        extractor = DatetimeFeatures(df, "nonexistent")
        extractor.filter_data()


def test_invalid_feature(spark):
    """Invalid feature raises error"""
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-01-02", 2)], ["timestamp", "value"]
    )

    with pytest.raises(ValueError, match="Invalid features"):
        extractor = DatetimeFeatures(df, "timestamp", features=["invalid_feature"])
        extractor.filter_data()


def test_default_features(spark):
    """Default features are year, month, day, weekday"""
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-01-02", 2)], ["timestamp", "value"]
    )

    extractor = DatetimeFeatures(df, "timestamp")
    result_df = extractor.filter_data()

    assert "year" in result_df.columns
    assert "month" in result_df.columns
    assert "day" in result_df.columns
    assert "weekday" in result_df.columns

    first_row = result_df.first()
    assert first_row["year"] == 2024
    assert first_row["month"] == 1
    assert first_row["day"] == 1


def test_year_month_extraction(spark):
    """Year and month extraction"""
    df = spark.createDataFrame(
        [("2024-03-15", 1), ("2023-12-25", 2), ("2025-06-01", 3)],
        ["timestamp", "value"],
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["year", "month"])
    result_df = extractor.filter_data()
    rows = result_df.orderBy("value").collect()

    assert rows[0]["year"] == 2024
    assert rows[0]["month"] == 3
    assert rows[1]["year"] == 2023
    assert rows[1]["month"] == 12
    assert rows[2]["year"] == 2025
    assert rows[2]["month"] == 6


def test_weekday_extraction(spark):
    """Weekday extraction (0=Monday, 6=Sunday)"""
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-01-02", 2), ("2024-01-03", 3)],
        ["timestamp", "value"],
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["weekday"])
    result_df = extractor.filter_data()
    rows = result_df.orderBy("value").collect()

    assert rows[0]["weekday"] == 0  # Monday
    assert rows[1]["weekday"] == 1  # Tuesday
    assert rows[2]["weekday"] == 2  # Wednesday


def test_is_weekend(spark):
    """Weekend detection"""
    df = spark.createDataFrame(
        [
            ("2024-01-05", 1),  # Friday
            ("2024-01-06", 2),  # Saturday
            ("2024-01-07", 3),  # Sunday
            ("2024-01-08", 4),  # Monday
        ],
        ["timestamp", "value"],
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["is_weekend"])
    result_df = extractor.filter_data()
    rows = result_df.orderBy("value").collect()

    assert rows[0]["is_weekend"] == False  # Friday
    assert rows[1]["is_weekend"] == True  # Saturday
    assert rows[2]["is_weekend"] == True  # Sunday
    assert rows[3]["is_weekend"] == False  # Monday


def test_hour_minute_second(spark):
    """Hour, minute, second extraction"""
    df = spark.createDataFrame(
        [("2024-01-01 14:30:45", 1), ("2024-01-01 08:15:30", 2)],
        ["timestamp", "value"],
    )

    extractor = DatetimeFeatures(
        df, "timestamp", features=["hour", "minute", "second"]
    )
    result_df = extractor.filter_data()
    rows = result_df.orderBy("value").collect()

    assert rows[0]["hour"] == 14
    assert rows[0]["minute"] == 30
    assert rows[0]["second"] == 45
    assert rows[1]["hour"] == 8
    assert rows[1]["minute"] == 15
    assert rows[1]["second"] == 30


def test_quarter(spark):
    """Quarter extraction"""
    df = spark.createDataFrame(
        [
            ("2024-01-15", 1),
            ("2024-04-15", 2),
            ("2024-07-15", 3),
            ("2024-10-15", 4),
        ],
        ["timestamp", "value"],
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["quarter"])
    result_df = extractor.filter_data()
    rows = result_df.orderBy("value").collect()

    assert rows[0]["quarter"] == 1
    assert rows[1]["quarter"] == 2
    assert rows[2]["quarter"] == 3
    assert rows[3]["quarter"] == 4


def test_day_name(spark):
    """Day name extraction"""
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-01-06", 2)], ["timestamp", "value"]
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["day_name"])
    result_df = extractor.filter_data()
    rows = result_df.orderBy("value").collect()

    assert rows[0]["day_name"] == "Monday"
    assert rows[1]["day_name"] == "Saturday"


def test_month_boundaries(spark):
    """Month start/end detection"""
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-01-15", 2), ("2024-01-31", 3)],
        ["timestamp", "value"],
    )

    extractor = DatetimeFeatures(
        df, "timestamp", features=["is_month_start", "is_month_end"]
    )
    result_df = extractor.filter_data()
    rows = result_df.orderBy("value").collect()

    assert rows[0]["is_month_start"] == True
    assert rows[0]["is_month_end"] == False
    assert rows[1]["is_month_start"] == False
    assert rows[1]["is_month_end"] == False
    assert rows[2]["is_month_start"] == False
    assert rows[2]["is_month_end"] == True


def test_prefix(spark):
    """Prefix is added to column names"""
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-01-02", 2)], ["timestamp", "value"]
    )

    extractor = DatetimeFeatures(
        df, "timestamp", features=["year", "month"], prefix="ts"
    )
    result_df = extractor.filter_data()

    assert "ts_year" in result_df.columns
    assert "ts_month" in result_df.columns
    assert "year" not in result_df.columns
    assert "month" not in result_df.columns


def test_preserves_original_columns(spark):
    """Original columns are preserved"""
    df = spark.createDataFrame(
        [("2024-01-01", 1, "A"), ("2024-01-02", 2, "B")],
        ["timestamp", "value", "category"],
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["year"])
    result_df = extractor.filter_data()

    assert "timestamp" in result_df.columns
    assert "value" in result_df.columns
    assert "category" in result_df.columns
    rows = result_df.orderBy("value").collect()
    assert rows[0]["value"] == 1
    assert rows[1]["value"] == 2


def test_all_features(spark):
    """All available features can be extracted"""
    df = spark.createDataFrame(
        [("2024-01-01", 1), ("2024-01-02", 2)], ["timestamp", "value"]
    )

    extractor = DatetimeFeatures(df, "timestamp", features=AVAILABLE_FEATURES)
    result_df = extractor.filter_data()

    for feature in AVAILABLE_FEATURES:
        assert feature in result_df.columns, f"Feature '{feature}' not found in result"


def test_system_type():
    """Test that system_type returns SystemType.PYSPARK"""
    assert DatetimeFeatures.system_type() == SystemType.PYSPARK


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = DatetimeFeatures.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = DatetimeFeatures.settings()
    assert isinstance(settings, dict)
    assert settings == {}
