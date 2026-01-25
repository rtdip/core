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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.mixed_type_separation import (
    MixedTypeSeparation,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


def test_empty_df():
    """Empty DataFrame"""
    empty_df = pd.DataFrame(columns=["TagName", "Value"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        separator = MixedTypeSeparation(empty_df, "Value")
        separator.apply()


def test_column_not_exists():
    """Column does not exist"""
    data = {
        "TagName": ["Tag_A", "Tag_B"],
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Column 'NonExistent' does not exist"):
        separator = MixedTypeSeparation(df, "NonExistent")
        separator.apply()


def test_all_numeric_values():
    """All numeric values - no separation needed"""
    data = {
        "TagName": ["A", "B", "C"],
        "Value": [1.0, 2.5, 3.14],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value")
    result_df = separator.apply()

    assert "Value_str" in result_df.columns
    assert (result_df["Value_str"] == "NaN").all()
    assert list(result_df["Value"]) == [1.0, 2.5, 3.14]


def test_all_string_values():
    """All string (non-numeric) values"""
    data = {
        "TagName": ["A", "B", "C"],
        "Value": ["Bad", "Error", "N/A"],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value", placeholder=-1)
    result_df = separator.apply()

    assert "Value_str" in result_df.columns
    assert list(result_df["Value_str"]) == ["Bad", "Error", "N/A"]
    assert (result_df["Value"] == -1).all()


def test_mixed_values():
    """Mixed numeric and string values"""
    data = {
        "TagName": ["A", "B", "C", "D"],
        "Value": [3.14, "Bad", 100, "Error"],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value", placeholder=-1)
    result_df = separator.apply()

    assert "Value_str" in result_df.columns
    assert result_df.loc[0, "Value"] == 3.14
    assert result_df.loc[0, "Value_str"] == "NaN"
    assert result_df.loc[1, "Value"] == -1
    assert result_df.loc[1, "Value_str"] == "Bad"
    assert result_df.loc[2, "Value"] == 100
    assert result_df.loc[2, "Value_str"] == "NaN"
    assert result_df.loc[3, "Value"] == -1
    assert result_df.loc[3, "Value_str"] == "Error"


def test_numeric_strings():
    """Numeric values stored as strings should be converted"""
    data = {
        "TagName": ["A", "B", "C", "D"],
        "Value": ["3.14", "1e-5", "-100", "Bad"],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value", placeholder=-1)
    result_df = separator.apply()

    assert result_df.loc[0, "Value"] == 3.14
    assert result_df.loc[0, "Value_str"] == "NaN"
    assert result_df.loc[1, "Value"] == 1e-5
    assert result_df.loc[1, "Value_str"] == "NaN"
    assert result_df.loc[2, "Value"] == -100.0
    assert result_df.loc[2, "Value_str"] == "NaN"
    assert result_df.loc[3, "Value"] == -1
    assert result_df.loc[3, "Value_str"] == "Bad"


def test_custom_placeholder():
    """Custom placeholder value"""
    data = {
        "TagName": ["A", "B"],
        "Value": [10.0, "Error"],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value", placeholder=-999)
    result_df = separator.apply()

    assert result_df.loc[1, "Value"] == -999


def test_custom_string_fill():
    """Custom string fill value"""
    data = {
        "TagName": ["A", "B"],
        "Value": [10.0, "Error"],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value", string_fill="NUMERIC")
    result_df = separator.apply()

    assert result_df.loc[0, "Value_str"] == "NUMERIC"
    assert result_df.loc[1, "Value_str"] == "Error"


def test_custom_suffix():
    """Custom suffix for string column"""
    data = {
        "TagName": ["A", "B"],
        "Value": [10.0, "Error"],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value", suffix="_text")
    result_df = separator.apply()

    assert "Value_text" in result_df.columns
    assert "Value_str" not in result_df.columns


def test_preserves_other_columns():
    """Ensures other columns are preserved"""
    data = {
        "TagName": ["Tag_A", "Tag_B"],
        "EventTime": ["2024-01-02 20:03:46", "2024-01-02 16:00:12"],
        "Status": ["Good", "Bad"],
        "Value": [1.0, "Error"],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value")
    result_df = separator.apply()

    assert "TagName" in result_df.columns
    assert "EventTime" in result_df.columns
    assert "Status" in result_df.columns
    assert "Value" in result_df.columns
    assert "Value_str" in result_df.columns


def test_null_values():
    """Column with null values"""
    data = {
        "TagName": ["A", "B", "C"],
        "Value": [1.0, None, "Bad"],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value", placeholder=-1)
    result_df = separator.apply()

    assert result_df.loc[0, "Value"] == 1.0
    # None is not a non-numeric string, so it stays as-is
    assert pd.isna(result_df.loc[1, "Value"]) or result_df.loc[1, "Value"] is None
    assert result_df.loc[2, "Value"] == -1


def test_special_string_values():
    """Special string values like whitespace and empty strings"""
    data = {
        "TagName": ["A", "B", "C"],
        "Value": [1.0, "", "  "],
    }
    df = pd.DataFrame(data)
    separator = MixedTypeSeparation(df, "Value", placeholder=-1)
    result_df = separator.apply()

    assert result_df.loc[0, "Value"] == 1.0
    # Empty string and whitespace are non-numeric strings
    assert result_df.loc[1, "Value"] == -1
    assert result_df.loc[1, "Value_str"] == ""
    assert result_df.loc[2, "Value"] == -1
    assert result_df.loc[2, "Value_str"] == "  "


def test_does_not_modify_original():
    """Ensures original DataFrame is not modified"""
    data = {
        "TagName": ["A", "B"],
        "Value": [1.0, "Bad"],
    }
    df = pd.DataFrame(data)
    original_df = df.copy()

    separator = MixedTypeSeparation(df, "Value")
    result_df = separator.apply()

    pd.testing.assert_frame_equal(df, original_df)
    assert "Value_str" not in df.columns


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert MixedTypeSeparation.system_type() == SystemType.PYTHON


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = MixedTypeSeparation.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = MixedTypeSeparation.settings()
    assert isinstance(settings, dict)
    assert settings == {}
