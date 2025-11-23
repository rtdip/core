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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.lag_features import (
    LagFeatures,
)


def test_empty_df():
    """Empty DataFrame raises error"""
    empty_df = pd.DataFrame(columns=["date", "value"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        lag_creator = LagFeatures(empty_df, value_column="value")
        lag_creator.apply()


def test_column_not_exists():
    """Non-existent value column raises error"""
    df = pd.DataFrame({"date": [1, 2, 3], "value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        lag_creator = LagFeatures(df, value_column="nonexistent")
        lag_creator.apply()


def test_group_column_not_exists():
    """Non-existent group column raises error"""
    df = pd.DataFrame({"date": [1, 2, 3], "value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Group column 'group' does not exist"):
        lag_creator = LagFeatures(df, value_column="value", group_columns=["group"])
        lag_creator.apply()


def test_invalid_lags():
    """Invalid lags raise error"""
    df = pd.DataFrame({"value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Lags must be a non-empty list"):
        lag_creator = LagFeatures(df, value_column="value", lags=[])
        lag_creator.apply()

    with pytest.raises(ValueError, match="Lags must be a non-empty list"):
        lag_creator = LagFeatures(df, value_column="value", lags=[0])
        lag_creator.apply()

    with pytest.raises(ValueError, match="Lags must be a non-empty list"):
        lag_creator = LagFeatures(df, value_column="value", lags=[-1])
        lag_creator.apply()


def test_default_lags():
    """Default lags are [1, 2, 3]"""
    df = pd.DataFrame({"value": [10, 20, 30, 40, 50]})

    lag_creator = LagFeatures(df, value_column="value")
    result = lag_creator.apply()

    assert "lag_1" in result.columns
    assert "lag_2" in result.columns
    assert "lag_3" in result.columns


def test_simple_lag():
    """Simple lag without groups"""
    df = pd.DataFrame({"value": [10, 20, 30, 40, 50]})

    lag_creator = LagFeatures(df, value_column="value", lags=[1, 2])
    result = lag_creator.apply()

    # lag_1 should be [NaN, 10, 20, 30, 40]
    assert pd.isna(result["lag_1"].iloc[0])
    assert result["lag_1"].iloc[1] == 10
    assert result["lag_1"].iloc[4] == 40

    # lag_2 should be [NaN, NaN, 10, 20, 30]
    assert pd.isna(result["lag_2"].iloc[0])
    assert pd.isna(result["lag_2"].iloc[1])
    assert result["lag_2"].iloc[2] == 10


def test_lag_with_groups():
    """Lags are computed within groups"""
    df = pd.DataFrame(
        {
            "group": ["A", "A", "A", "B", "B", "B"],
            "value": [10, 20, 30, 100, 200, 300],
        }
    )

    lag_creator = LagFeatures(
        df, value_column="value", group_columns=["group"], lags=[1]
    )
    result = lag_creator.apply()

    # Group A: lag_1 should be [NaN, 10, 20]
    group_a = result[result["group"] == "A"]
    assert pd.isna(group_a["lag_1"].iloc[0])
    assert group_a["lag_1"].iloc[1] == 10
    assert group_a["lag_1"].iloc[2] == 20

    # Group B: lag_1 should be [NaN, 100, 200]
    group_b = result[result["group"] == "B"]
    assert pd.isna(group_b["lag_1"].iloc[0])
    assert group_b["lag_1"].iloc[1] == 100
    assert group_b["lag_1"].iloc[2] == 200


def test_multiple_group_columns():
    """Lags with multiple group columns"""
    df = pd.DataFrame(
        {
            "region": ["R1", "R1", "R1", "R1"],
            "product": ["A", "A", "B", "B"],
            "value": [10, 20, 100, 200],
        }
    )

    lag_creator = LagFeatures(
        df, value_column="value", group_columns=["region", "product"], lags=[1]
    )
    result = lag_creator.apply()

    # R1-A group: lag_1 should be [NaN, 10]
    r1a = result[(result["region"] == "R1") & (result["product"] == "A")]
    assert pd.isna(r1a["lag_1"].iloc[0])
    assert r1a["lag_1"].iloc[1] == 10

    # R1-B group: lag_1 should be [NaN, 100]
    r1b = result[(result["region"] == "R1") & (result["product"] == "B")]
    assert pd.isna(r1b["lag_1"].iloc[0])
    assert r1b["lag_1"].iloc[1] == 100


def test_custom_prefix():
    """Custom prefix for lag columns"""
    df = pd.DataFrame({"value": [10, 20, 30]})

    lag_creator = LagFeatures(df, value_column="value", lags=[1], prefix="shifted")
    result = lag_creator.apply()

    assert "shifted_1" in result.columns
    assert "lag_1" not in result.columns


def test_preserves_other_columns():
    """Other columns are preserved"""
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=3),
            "category": ["A", "B", "C"],
            "value": [10, 20, 30],
        }
    )

    lag_creator = LagFeatures(df, value_column="value", lags=[1])
    result = lag_creator.apply()

    assert "date" in result.columns
    assert "category" in result.columns
    assert list(result["category"]) == ["A", "B", "C"]
