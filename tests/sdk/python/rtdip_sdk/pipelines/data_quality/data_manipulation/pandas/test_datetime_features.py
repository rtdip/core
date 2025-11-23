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
import pandas as pd

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.datetime_features import (
    DatetimeFeatures,
    AVAILABLE_FEATURES,
)


def test_empty_df():
    """Empty DataFrame raises error"""
    empty_df = pd.DataFrame(columns=["timestamp", "value"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        extractor = DatetimeFeatures(empty_df, "timestamp")
        extractor.apply()


def test_column_not_exists():
    """Non-existent column raises error"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "value": [1, 2, 3],
        }
    )

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        extractor = DatetimeFeatures(df, "nonexistent")
        extractor.apply()


def test_invalid_feature():
    """Invalid feature raises error"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "value": [1, 2, 3],
        }
    )

    with pytest.raises(ValueError, match="Invalid features"):
        extractor = DatetimeFeatures(df, "timestamp", features=["invalid_feature"])
        extractor.apply()


def test_default_features():
    """Default features are year, month, day, weekday"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "value": [1, 2, 3],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp")
    result_df = extractor.apply()

    assert "year" in result_df.columns
    assert "month" in result_df.columns
    assert "day" in result_df.columns
    assert "weekday" in result_df.columns
    assert result_df["year"].iloc[0] == 2024
    assert result_df["month"].iloc[0] == 1
    assert result_df["day"].iloc[0] == 1


def test_year_month_extraction():
    """Year and month extraction"""
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-03-15", "2023-12-25", "2025-06-01"]),
            "value": [1, 2, 3],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["year", "month"])
    result_df = extractor.apply()

    assert list(result_df["year"]) == [2024, 2023, 2025]
    assert list(result_df["month"]) == [3, 12, 6]


def test_weekday_extraction():
    """Weekday extraction (0=Monday, 6=Sunday)"""
    df = pd.DataFrame(
        {
            # Monday, Tuesday, Wednesday
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
            "value": [1, 2, 3],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["weekday"])
    result_df = extractor.apply()

    assert list(result_df["weekday"]) == [0, 1, 2]  # Mon, Tue, Wed


def test_is_weekend():
    """Weekend detection"""
    df = pd.DataFrame(
        {
            # Friday, Saturday, Sunday, Monday
            "timestamp": pd.to_datetime(
                ["2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08"]
            ),
            "value": [1, 2, 3, 4],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["is_weekend"])
    result_df = extractor.apply()

    assert list(result_df["is_weekend"]) == [False, True, True, False]


def test_hour_minute_second():
    """Hour, minute, second extraction"""
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01 14:30:45", "2024-01-01 08:15:30"]),
            "value": [1, 2],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["hour", "minute", "second"])
    result_df = extractor.apply()

    assert list(result_df["hour"]) == [14, 8]
    assert list(result_df["minute"]) == [30, 15]
    assert list(result_df["second"]) == [45, 30]


def test_quarter():
    """Quarter extraction"""
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-15", "2024-04-15", "2024-07-15", "2024-10-15"]
            ),
            "value": [1, 2, 3, 4],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["quarter"])
    result_df = extractor.apply()

    assert list(result_df["quarter"]) == [1, 2, 3, 4]


def test_day_name():
    """Day name extraction"""
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2024-01-01", "2024-01-06"]
            ),  # Monday, Saturday
            "value": [1, 2],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["day_name"])
    result_df = extractor.apply()

    assert list(result_df["day_name"]) == ["Monday", "Saturday"]


def test_month_boundaries():
    """Month start/end detection"""
    df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2024-01-01", "2024-01-15", "2024-01-31"]),
            "value": [1, 2, 3],
        }
    )

    extractor = DatetimeFeatures(
        df, "timestamp", features=["is_month_start", "is_month_end"]
    )
    result_df = extractor.apply()

    assert list(result_df["is_month_start"]) == [True, False, False]
    assert list(result_df["is_month_end"]) == [False, False, True]


def test_string_datetime_column():
    """String datetime column is auto-converted"""
    df = pd.DataFrame(
        {
            "timestamp": ["2024-01-01", "2024-02-01", "2024-03-01"],
            "value": [1, 2, 3],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["year", "month"])
    result_df = extractor.apply()

    assert list(result_df["year"]) == [2024, 2024, 2024]
    assert list(result_df["month"]) == [1, 2, 3]


def test_prefix():
    """Prefix is added to column names"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "value": [1, 2, 3],
        }
    )

    extractor = DatetimeFeatures(
        df, "timestamp", features=["year", "month"], prefix="ts"
    )
    result_df = extractor.apply()

    assert "ts_year" in result_df.columns
    assert "ts_month" in result_df.columns
    assert "year" not in result_df.columns
    assert "month" not in result_df.columns


def test_preserves_original_columns():
    """Original columns are preserved"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "value": [1, 2, 3],
            "category": ["A", "B", "C"],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=["year"])
    result_df = extractor.apply()

    assert "timestamp" in result_df.columns
    assert "value" in result_df.columns
    assert "category" in result_df.columns
    assert list(result_df["value"]) == [1, 2, 3]


def test_all_features():
    """All available features can be extracted"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="D"),
            "value": [1, 2, 3],
        }
    )

    extractor = DatetimeFeatures(df, "timestamp", features=AVAILABLE_FEATURES)
    result_df = extractor.apply()

    for feature in AVAILABLE_FEATURES:
        assert feature in result_df.columns, f"Feature '{feature}' not found in result"
