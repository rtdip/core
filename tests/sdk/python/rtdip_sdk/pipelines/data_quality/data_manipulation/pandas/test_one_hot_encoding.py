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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.one_hot_encoding import (
    OneHotEncoding,
)


def test_empty_df():
    """Empty DataFrame"""
    empty_df = pd.DataFrame(columns=["TagName", "EventTime", "Status", "Value"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        encoder = OneHotEncoding(empty_df, "TagName")
        encoder.filter_data()


def test_single_unique_value():
    """Single Unique Value"""
    data = {
        "TagName": ["A2PS64V0J.:ZUX09R", "A2PS64V0J.:ZUX09R"],
        "EventTime": ["2024-01-02 20:03:46", "2024-01-02 16:00:12"],
        "Status": ["Good", "Good"],
        "Value": [0.34, 0.15],
    }
    df = pd.DataFrame(data)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.filter_data()

    assert "TagName_A2PS64V0J.:ZUX09R" in result_df.columns, (
        "Expected one-hot encoded column not found."
    )
    assert (result_df["TagName_A2PS64V0J.:ZUX09R"] == True).all(), (
        "Expected all True for single unique value."
    )


def test_null_values():
    """Column with Null Values"""
    data = {
        "TagName": ["A2PS64V0J.:ZUX09R", None],
        "EventTime": ["2024-01-02 20:03:46", "2024-01-02 16:00:12"],
        "Status": ["Good", "Good"],
        "Value": [0.34, 0.15],
    }
    df = pd.DataFrame(data)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.filter_data()

    # pd.get_dummies creates columns for non-null values only by default
    assert "TagName_A2PS64V0J.:ZUX09R" in result_df.columns, (
        "Expected one-hot encoded column not found."
    )


def test_multiple_unique_values():
    """Multiple Unique Values"""
    data = {
        "TagName": ["Tag_A", "Tag_B", "Tag_C", "Tag_A"],
        "EventTime": ["2024-01-02 20:03:46", "2024-01-02 16:00:12", "2024-01-02 12:00:00", "2024-01-02 08:00:00"],
        "Status": ["Good", "Good", "Good", "Good"],
        "Value": [1.0, 2.0, 3.0, 4.0],
    }
    df = pd.DataFrame(data)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.filter_data()

    # Check all expected columns exist
    assert "TagName_Tag_A" in result_df.columns
    assert "TagName_Tag_B" in result_df.columns
    assert "TagName_Tag_C" in result_df.columns

    # Check one-hot property: each row has exactly one True in TagName columns
    tag_columns = [col for col in result_df.columns if col.startswith("TagName_")]
    row_sums = result_df[tag_columns].sum(axis=1)
    assert (row_sums == 1).all(), "Each row should have exactly one hot-encoded value."


def test_large_unique_values():
    """Large Number of Unique Values"""
    data = {
        "TagName": [f"Tag_{i}" for i in range(1000)],
        "EventTime": [f"2024-01-02 20:03:{i:02d}" for i in range(1000)],
        "Status": ["Good"] * 1000,
        "Value": [i * 1.0 for i in range(1000)],
    }
    df = pd.DataFrame(data)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.filter_data()

    # Original columns (minus TagName) + 1000 one-hot columns
    expected_columns = 3 + 1000  # EventTime, Status, Value + 1000 tags
    assert len(result_df.columns) == expected_columns, (
        f"Expected {expected_columns} columns, got {len(result_df.columns)}."
    )


def test_special_characters():
    """Special Characters in Column Values"""
    data = {
        "TagName": ["A2PS64V0J.:ZUX09R", "@Special#Tag!"],
        "EventTime": ["2024-01-02 20:03:46", "2024-01-02 16:00:12"],
        "Status": ["Good", "Good"],
        "Value": [0.34, 0.15],
    }
    df = pd.DataFrame(data)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.filter_data()

    assert "TagName_A2PS64V0J.:ZUX09R" in result_df.columns
    assert "TagName_@Special#Tag!" in result_df.columns


def test_column_not_exists():
    """Column does not exist"""
    data = {
        "TagName": ["Tag_A", "Tag_B"],
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Column 'NonExistent' does not exist"):
        encoder = OneHotEncoding(df, "NonExistent")
        encoder.filter_data()


def test_preserves_other_columns():
    """Ensures other columns are preserved"""
    data = {
        "TagName": ["Tag_A", "Tag_B"],
        "EventTime": ["2024-01-02 20:03:46", "2024-01-02 16:00:12"],
        "Status": ["Good", "Bad"],
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)
    encoder = OneHotEncoding(df, "TagName")
    result_df = encoder.filter_data()

    # Original columns except TagName should be preserved
    assert "EventTime" in result_df.columns
    assert "Status" in result_df.columns
    assert "Value" in result_df.columns
    # Original TagName column should be removed
    assert "TagName" not in result_df.columns
