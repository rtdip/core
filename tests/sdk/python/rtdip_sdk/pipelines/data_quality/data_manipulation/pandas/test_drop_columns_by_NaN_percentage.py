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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.drop_columns_by_NaN_percentage import (
    DropByNaNPercentage,
)


def test_empty_df():
    """Empty DataFrame should raise error"""
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        dropper = DropByNaNPercentage(empty_df, nan_threshold=0.5)
        dropper.apply()


def test_none_df():
    """None passed as DataFrame should raise error"""
    with pytest.raises(ValueError, match="The DataFrame is empty."):
        dropper = DropByNaNPercentage(None, nan_threshold=0.5)
        dropper.apply()


def test_negative_threshold():
    """Negative NaN threshold should raise error"""
    df = pd.DataFrame({"a": [1, 2, 3]})

    with pytest.raises(ValueError, match="NaN Threshold is negative."):
        dropper = DropByNaNPercentage(df, nan_threshold=-0.1)
        dropper.apply()


def test_drop_columns_by_nan_percentage():
    """Drop columns exceeding threshold"""
    data = {
        "a": [1, None, 3],          # 33% NaN -> keep
        "b": [None, None, None],    # 100% NaN -> drop
        "c": [7, 8, 9],             # 0% NaN -> keep
        "d": [1, None, None],       # 66% NaN -> drop at threshold 0.5
    }
    df = pd.DataFrame(data)

    dropper = DropByNaNPercentage(df, nan_threshold=0.5)
    result_df = dropper.apply()

    assert list(result_df.columns) == ["a", "c"]
    pd.testing.assert_series_equal(result_df["a"], df["a"])
    pd.testing.assert_series_equal(result_df["c"], df["c"])


def test_threshold_1_keeps_all_columns():
    """Threshold = 1 means only 100% NaN columns removed"""
    data = {
        "a": [np.nan, 1, 2],   # 33% NaN -> keep
        "b": [np.nan, np.nan, np.nan],  # 100% -> drop
        "c": [3, 4, 5],        # 0% -> keep
    }
    df = pd.DataFrame(data)

    dropper = DropByNaNPercentage(df, nan_threshold=1.0)
    result_df = dropper.apply()

    assert list(result_df.columns) == ["a", "c"]


def test_threshold_0_removes_all_columns_with_any_nan():
    """Threshold = 0 removes every column that has any NaN"""
    data = {
        "a": [1, np.nan, 3],   # contains NaN → drop
        "b": [4, 5, 6],        # no NaN → keep
        "c": [np.nan, np.nan, 9],  # contains NaN → drop
    }
    df = pd.DataFrame(data)

    dropper = DropByNaNPercentage(df, nan_threshold=0.0)
    result_df = dropper.apply()

    assert list(result_df.columns) == ["b"]


def test_no_columns_dropped():
    """No column exceeds threshold → expect identical DataFrame"""
    df = pd.DataFrame({
        "a": [1, 2, 3],
        "b": [4.0, 5.0, 6.0],
        "c": ["x", "y", "z"],
    })

    dropper = DropByNaNPercentage(df, nan_threshold=0.5)
    result_df = dropper.apply()

    pd.testing.assert_frame_equal(result_df, df)


def test_original_df_not_modified():
    """Ensure original DataFrame remains unchanged"""
    df = pd.DataFrame({
        "a": [1, None, 3],   # 33% NaN
        "b": [None, None, None]  # 100% NaN → drop
    })

    df_copy = df.copy()

    dropper = DropByNaNPercentage(df, nan_threshold=0.5)
    _ = dropper.apply()

    # original must stay untouched
    pd.testing.assert_frame_equal(df, df_copy)