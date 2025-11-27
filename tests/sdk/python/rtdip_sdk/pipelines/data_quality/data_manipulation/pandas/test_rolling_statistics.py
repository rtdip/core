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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.rolling_statistics import (
    RollingStatistics,
    AVAILABLE_STATISTICS,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


def test_empty_df():
    """Empty DataFrame raises error"""
    empty_df = pd.DataFrame(columns=["date", "value"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        roller = RollingStatistics(empty_df, value_column="value")
        roller.apply()


def test_column_not_exists():
    """Non-existent value column raises error"""
    df = pd.DataFrame({"date": [1, 2, 3], "value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Column 'nonexistent' does not exist"):
        roller = RollingStatistics(df, value_column="nonexistent")
        roller.apply()


def test_group_column_not_exists():
    """Non-existent group column raises error"""
    df = pd.DataFrame({"date": [1, 2, 3], "value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Group column 'group' does not exist"):
        roller = RollingStatistics(df, value_column="value", group_columns=["group"])
        roller.apply()


def test_invalid_statistics():
    """Invalid statistics raise error"""
    df = pd.DataFrame({"value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Invalid statistics"):
        roller = RollingStatistics(df, value_column="value", statistics=["invalid"])
        roller.apply()


def test_invalid_windows():
    """Invalid windows raise error"""
    df = pd.DataFrame({"value": [10, 20, 30]})

    with pytest.raises(ValueError, match="Windows must be a non-empty list"):
        roller = RollingStatistics(df, value_column="value", windows=[])
        roller.apply()

    with pytest.raises(ValueError, match="Windows must be a non-empty list"):
        roller = RollingStatistics(df, value_column="value", windows=[0])
        roller.apply()


def test_default_windows_and_statistics():
    """Default windows are [3, 6, 12] and statistics are [mean, std]"""
    df = pd.DataFrame({"value": list(range(15))})

    roller = RollingStatistics(df, value_column="value")
    result = roller.apply()

    assert "rolling_mean_3" in result.columns
    assert "rolling_std_3" in result.columns
    assert "rolling_mean_6" in result.columns
    assert "rolling_std_6" in result.columns
    assert "rolling_mean_12" in result.columns
    assert "rolling_std_12" in result.columns


def test_rolling_mean():
    """Rolling mean is computed correctly"""
    df = pd.DataFrame({"value": [10, 20, 30, 40, 50]})

    roller = RollingStatistics(
        df, value_column="value", windows=[3], statistics=["mean"]
    )
    result = roller.apply()

    # With min_periods=1:
    # [10] -> mean=10
    # [10, 20] -> mean=15
    # [10, 20, 30] -> mean=20
    # [20, 30, 40] -> mean=30
    # [30, 40, 50] -> mean=40
    assert result["rolling_mean_3"].iloc[0] == 10
    assert result["rolling_mean_3"].iloc[1] == 15
    assert result["rolling_mean_3"].iloc[2] == 20
    assert result["rolling_mean_3"].iloc[3] == 30
    assert result["rolling_mean_3"].iloc[4] == 40


def test_rolling_min_max():
    """Rolling min and max are computed correctly"""
    df = pd.DataFrame({"value": [10, 5, 30, 20, 50]})

    roller = RollingStatistics(
        df, value_column="value", windows=[3], statistics=["min", "max"]
    )
    result = roller.apply()

    # Window 3 rolling min: [10, 5, 5, 5, 20]
    # Window 3 rolling max: [10, 10, 30, 30, 50]
    assert result["rolling_min_3"].iloc[2] == 5  # min of [10, 5, 30]
    assert result["rolling_max_3"].iloc[2] == 30  # max of [10, 5, 30]


def test_rolling_std():
    """Rolling std is computed correctly"""
    df = pd.DataFrame({"value": [10, 10, 10, 10, 10]})

    roller = RollingStatistics(
        df, value_column="value", windows=[3], statistics=["std"]
    )
    result = roller.apply()

    # All same values -> std should be 0 (or NaN for first value)
    assert result["rolling_std_3"].iloc[4] == 0


def test_rolling_with_groups():
    """Rolling statistics are computed within groups"""
    df = pd.DataFrame(
        {
            "group": ["A", "A", "A", "B", "B", "B"],
            "value": [10, 20, 30, 100, 200, 300],
        }
    )

    roller = RollingStatistics(
        df,
        value_column="value",
        group_columns=["group"],
        windows=[2],
        statistics=["mean"],
    )
    result = roller.apply()

    # Group A: rolling_mean_2 should be [10, 15, 25]
    group_a = result[result["group"] == "A"]
    assert group_a["rolling_mean_2"].iloc[0] == 10
    assert group_a["rolling_mean_2"].iloc[1] == 15
    assert group_a["rolling_mean_2"].iloc[2] == 25

    # Group B: rolling_mean_2 should be [100, 150, 250]
    group_b = result[result["group"] == "B"]
    assert group_b["rolling_mean_2"].iloc[0] == 100
    assert group_b["rolling_mean_2"].iloc[1] == 150
    assert group_b["rolling_mean_2"].iloc[2] == 250


def test_multiple_windows():
    """Multiple windows create multiple columns"""
    df = pd.DataFrame({"value": list(range(10))})

    roller = RollingStatistics(
        df, value_column="value", windows=[2, 3], statistics=["mean"]
    )
    result = roller.apply()

    assert "rolling_mean_2" in result.columns
    assert "rolling_mean_3" in result.columns


def test_all_statistics():
    """All available statistics can be computed"""
    df = pd.DataFrame({"value": list(range(10))})

    roller = RollingStatistics(
        df, value_column="value", windows=[3], statistics=AVAILABLE_STATISTICS
    )
    result = roller.apply()

    for stat in AVAILABLE_STATISTICS:
        assert f"rolling_{stat}_3" in result.columns


def test_preserves_other_columns():
    """Other columns are preserved"""
    df = pd.DataFrame(
        {
            "date": pd.date_range("2024-01-01", periods=5),
            "category": ["A", "B", "C", "D", "E"],
            "value": [10, 20, 30, 40, 50],
        }
    )

    roller = RollingStatistics(
        df, value_column="value", windows=[2], statistics=["mean"]
    )
    result = roller.apply()

    assert "date" in result.columns
    assert "category" in result.columns
    assert list(result["category"]) == ["A", "B", "C", "D", "E"]


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert RollingStatistics.system_type() == SystemType.PYTHON


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = RollingStatistics.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = RollingStatistics.settings()
    assert isinstance(settings, dict)
    assert settings == {}
