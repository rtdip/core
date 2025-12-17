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
import numpy as np

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.datetime_string_conversion import (
    DatetimeStringConversion,
)


def test_empty_df():
    """Empty DataFrame"""
    empty_df = pd.DataFrame(columns=["TagName", "EventTime"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        converter = DatetimeStringConversion(empty_df, "EventTime")
        converter.apply()


def test_column_not_exists():
    """Column does not exist"""
    data = {
        "TagName": ["Tag_A", "Tag_B"],
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Column 'EventTime' does not exist"):
        converter = DatetimeStringConversion(df, "EventTime")
        converter.apply()


def test_standard_format_with_microseconds():
    """Standard datetime format with microseconds"""
    data = {
        "EventTime": [
            "2024-01-02 20:03:46.123456",
            "2024-01-02 16:00:12.000001",
        ]
    }
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    assert "EventTime_DT" in result_df.columns
    assert result_df["EventTime_DT"].dtype == "datetime64[ns]"
    assert not result_df["EventTime_DT"].isna().any()


def test_standard_format_without_microseconds():
    """Standard datetime format without microseconds"""
    data = {
        "EventTime": [
            "2024-01-02 20:03:46",
            "2024-01-02 16:00:12",
        ]
    }
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    assert "EventTime_DT" in result_df.columns
    assert not result_df["EventTime_DT"].isna().any()


def test_trailing_zeros_stripped():
    """Timestamps with trailing .000 should be parsed correctly"""
    data = {
        "EventTime": [
            "2024-01-02 20:03:46.000",
            "2024-01-02 16:00:12.000",
        ]
    }
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime", strip_trailing_zeros=True)
    result_df = converter.apply()

    assert not result_df["EventTime_DT"].isna().any()
    assert result_df["EventTime_DT"].iloc[0] == pd.Timestamp("2024-01-02 20:03:46")


def test_mixed_formats():
    """Mixed datetime formats in same column"""
    data = {
        "EventTime": [
            "2024-01-02 20:03:46.000",
            "2024-01-02 16:00:12.123456",
            "2024-01-02 11:56:42",
        ]
    }
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    assert not result_df["EventTime_DT"].isna().any()


def test_custom_output_column():
    """Custom output column name"""
    data = {"EventTime": ["2024-01-02 20:03:46"]}
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime", output_column="Timestamp")
    result_df = converter.apply()

    assert "Timestamp" in result_df.columns
    assert "EventTime_DT" not in result_df.columns


def test_keep_original_true():
    """Original column is kept by default"""
    data = {"EventTime": ["2024-01-02 20:03:46"]}
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime", keep_original=True)
    result_df = converter.apply()

    assert "EventTime" in result_df.columns
    assert "EventTime_DT" in result_df.columns


def test_keep_original_false():
    """Original column is dropped when keep_original=False"""
    data = {"EventTime": ["2024-01-02 20:03:46"]}
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime", keep_original=False)
    result_df = converter.apply()

    assert "EventTime" not in result_df.columns
    assert "EventTime_DT" in result_df.columns


def test_invalid_datetime_string():
    """Invalid datetime strings result in NaT"""
    data = {
        "EventTime": [
            "2024-01-02 20:03:46",
            "invalid_datetime",
            "not_a_date",
        ]
    }
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    assert not pd.isna(result_df["EventTime_DT"].iloc[0])
    assert pd.isna(result_df["EventTime_DT"].iloc[1])
    assert pd.isna(result_df["EventTime_DT"].iloc[2])


def test_iso_format():
    """ISO 8601 format with T separator"""
    data = {
        "EventTime": [
            "2024-01-02T20:03:46",
            "2024-01-02T16:00:12.123456",
        ]
    }
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    assert not result_df["EventTime_DT"].isna().any()


def test_custom_formats():
    """Custom format list"""
    data = {
        "EventTime": [
            "02/01/2024 20:03:46",
            "03/01/2024 16:00:12",
        ]
    }
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime", formats=["%d/%m/%Y %H:%M:%S"])
    result_df = converter.apply()

    assert not result_df["EventTime_DT"].isna().any()
    assert result_df["EventTime_DT"].iloc[0].day == 2
    assert result_df["EventTime_DT"].iloc[0].month == 1


def test_preserves_other_columns():
    """Ensures other columns are preserved"""
    data = {
        "TagName": ["Tag_A", "Tag_B"],
        "EventTime": ["2024-01-02 20:03:46", "2024-01-02 16:00:12"],
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    assert "TagName" in result_df.columns
    assert "Value" in result_df.columns
    assert list(result_df["TagName"]) == ["Tag_A", "Tag_B"]
    assert list(result_df["Value"]) == [1.0, 2.0]


def test_does_not_modify_original():
    """Ensures original DataFrame is not modified"""
    data = {"EventTime": ["2024-01-02 20:03:46"]}
    df = pd.DataFrame(data)
    original_df = df.copy()

    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    pd.testing.assert_frame_equal(df, original_df)
    assert "EventTime_DT" not in df.columns


def test_null_values():
    """Null values in datetime column"""
    data = {"EventTime": ["2024-01-02 20:03:46", None, "2024-01-02 16:00:12"]}
    df = pd.DataFrame(data)
    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    assert not pd.isna(result_df["EventTime_DT"].iloc[0])
    assert pd.isna(result_df["EventTime_DT"].iloc[1])
    assert not pd.isna(result_df["EventTime_DT"].iloc[2])


def test_already_datetime():
    """Column already contains datetime objects (converted to string first)"""
    data = {"EventTime": pd.to_datetime(["2024-01-02 20:03:46", "2024-01-02 16:00:12"])}
    df = pd.DataFrame(data)
    # Convert to string to simulate the use case
    df["EventTime"] = df["EventTime"].astype(str)

    converter = DatetimeStringConversion(df, "EventTime")
    result_df = converter.apply()

    assert not result_df["EventTime_DT"].isna().any()
