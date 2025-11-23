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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.chronological_sort import (
    ChronologicalSort,
)


def test_empty_df():
    """Empty DataFrame"""
    empty_df = pd.DataFrame(columns=["TagName", "Timestamp"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        sorter = ChronologicalSort(empty_df, "Timestamp")
        sorter.apply()


def test_column_not_exists():
    """Column does not exist"""
    data = {
        "TagName": ["Tag_A", "Tag_B"],
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Column 'Timestamp' does not exist"):
        sorter = ChronologicalSort(df, "Timestamp")
        sorter.apply()


def test_group_column_not_exists():
    """Group column does not exist"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Group column 'sensor_id' does not exist"):
        sorter = ChronologicalSort(df, "Timestamp", group_columns=["sensor_id"])
        sorter.apply()


def test_invalid_na_position():
    """Invalid na_position parameter"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-01", "2024-01-02"]),
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Invalid na_position"):
        sorter = ChronologicalSort(df, "Timestamp", na_position="middle")
        sorter.apply()


def test_basic_sort_ascending():
    """Basic ascending sort"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"]),
        "Value": [30, 10, 20],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp", ascending=True)
    result_df = sorter.apply()

    expected_order = [10, 20, 30]
    assert list(result_df["Value"]) == expected_order
    assert result_df["Timestamp"].is_monotonic_increasing


def test_basic_sort_descending():
    """Basic descending sort"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"]),
        "Value": [30, 10, 20],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp", ascending=False)
    result_df = sorter.apply()

    expected_order = [30, 20, 10]
    assert list(result_df["Value"]) == expected_order
    assert result_df["Timestamp"].is_monotonic_decreasing


def test_sort_with_groups():
    """Sort within groups"""
    data = {
        "sensor_id": ["A", "A", "B", "B"],
        "Timestamp": pd.to_datetime([
            "2024-01-02", "2024-01-01",  # Group A (out of order)
            "2024-01-02", "2024-01-01",  # Group B (out of order)
        ]),
        "Value": [20, 10, 200, 100],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp", group_columns=["sensor_id"])
    result_df = sorter.apply()

    # Group A should come first, then Group B, each sorted by time
    assert list(result_df["sensor_id"]) == ["A", "A", "B", "B"]
    assert list(result_df["Value"]) == [10, 20, 100, 200]


def test_nat_values_last():
    """NaT values positioned last by default"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-02", None, "2024-01-01"]),
        "Value": [20, 0, 10],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp", na_position="last")
    result_df = sorter.apply()

    assert list(result_df["Value"]) == [10, 20, 0]
    assert pd.isna(result_df["Timestamp"].iloc[-1])


def test_nat_values_first():
    """NaT values positioned first"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-02", None, "2024-01-01"]),
        "Value": [20, 0, 10],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp", na_position="first")
    result_df = sorter.apply()

    assert list(result_df["Value"]) == [0, 10, 20]
    assert pd.isna(result_df["Timestamp"].iloc[0])


def test_reset_index_true():
    """Index is reset by default"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"]),
        "Value": [30, 10, 20],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp", reset_index=True)
    result_df = sorter.apply()

    assert list(result_df.index) == [0, 1, 2]


def test_reset_index_false():
    """Index is preserved when reset_index=False"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"]),
        "Value": [30, 10, 20],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp", reset_index=False)
    result_df = sorter.apply()

    # Original indices should be preserved (1, 2, 0 after sorting)
    assert list(result_df.index) == [1, 2, 0]


def test_already_sorted():
    """Already sorted data remains unchanged"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "Value": [10, 20, 30],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp")
    result_df = sorter.apply()

    assert list(result_df["Value"]) == [10, 20, 30]


def test_preserves_other_columns():
    """Ensures other columns are preserved"""
    data = {
        "TagName": ["C", "A", "B"],
        "Timestamp": pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"]),
        "Status": ["Good", "Bad", "Good"],
        "Value": [30, 10, 20],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp")
    result_df = sorter.apply()

    assert list(result_df["TagName"]) == ["A", "B", "C"]
    assert list(result_df["Status"]) == ["Bad", "Good", "Good"]
    assert list(result_df["Value"]) == [10, 20, 30]


def test_does_not_modify_original():
    """Ensures original DataFrame is not modified"""
    data = {
        "Timestamp": pd.to_datetime(["2024-01-03", "2024-01-01", "2024-01-02"]),
        "Value": [30, 10, 20],
    }
    df = pd.DataFrame(data)
    original_df = df.copy()

    sorter = ChronologicalSort(df, "Timestamp")
    result_df = sorter.apply()

    pd.testing.assert_frame_equal(df, original_df)


def test_with_microseconds():
    """Sort with microsecond precision"""
    data = {
        "Timestamp": pd.to_datetime([
            "2024-01-01 10:00:00.000003",
            "2024-01-01 10:00:00.000001",
            "2024-01-01 10:00:00.000002",
        ]),
        "Value": [3, 1, 2],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp")
    result_df = sorter.apply()

    assert list(result_df["Value"]) == [1, 2, 3]


def test_multiple_group_columns():
    """Sort with multiple group columns"""
    data = {
        "region": ["East", "East", "West", "West"],
        "sensor_id": ["A", "A", "A", "A"],
        "Timestamp": pd.to_datetime([
            "2024-01-02", "2024-01-01",
            "2024-01-02", "2024-01-01",
        ]),
        "Value": [20, 10, 200, 100],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(
        df, "Timestamp", group_columns=["region", "sensor_id"]
    )
    result_df = sorter.apply()

    # East group first, then West, each sorted by time
    assert list(result_df["region"]) == ["East", "East", "West", "West"]
    assert list(result_df["Value"]) == [10, 20, 100, 200]


def test_stable_sort():
    """Stable sort preserves order of equal timestamps"""
    data = {
        "Timestamp": pd.to_datetime([
            "2024-01-01", "2024-01-01", "2024-01-01"
        ]),
        "Order": [1, 2, 3],  # Original order
        "Value": [10, 20, 30],
    }
    df = pd.DataFrame(data)
    sorter = ChronologicalSort(df, "Timestamp")
    result_df = sorter.apply()

    # Original order should be preserved for equal timestamps
    assert list(result_df["Order"]) == [1, 2, 3]
