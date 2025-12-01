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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.select_columns_by_correlation import (
    SelectColumnsByCorrelation,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


def test_empty_df():
    """Empty DataFrame -> raises ValueError"""
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        selector = SelectColumnsByCorrelation(
            df=empty_df,
            columns_to_keep=["id"],
            target_col_name="target",
            correlation_threshold=0.6,
        )
        selector.apply()


def test_none_df():
    """DataFrame is None -> raises ValueError"""
    with pytest.raises(ValueError, match="The DataFrame is empty."):
        selector = SelectColumnsByCorrelation(
            df=None,
            columns_to_keep=["id"],
            target_col_name="target",
            correlation_threshold=0.6,
        )
        selector.apply()


def test_missing_target_column_raises():
    """Target column not present in DataFrame -> raises ValueError"""
    df = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "feature_2": [2, 3, 4],
        }
    )

    with pytest.raises(
        ValueError,
        match="Target column 'target' does not exist in the DataFrame.",
    ):
        selector = SelectColumnsByCorrelation(
            df=df,
            columns_to_keep=["feature_1"],
            target_col_name="target",
            correlation_threshold=0.5,
        )
        selector.apply()


def test_missing_columns_to_keep_raise():
    """Columns in columns_to_keep not present in DataFrame -> raises ValueError"""
    df = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "target": [1, 2, 3],
        }
    )

    with pytest.raises(
        ValueError,
        match="missing in the DataFrame",
    ):
        selector = SelectColumnsByCorrelation(
            df=df,
            columns_to_keep=["feature_1", "non_existing_column"],
            target_col_name="target",
            correlation_threshold=0.5,
        )
        selector.apply()


def test_invalid_correlation_threshold_raises():
    """Correlation threshold outside [0, 1] -> raises ValueError"""
    df = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "target": [1, 2, 3],
        }
    )

    # Negative threshold
    with pytest.raises(
        ValueError,
        match="correlation_threshold must be between 0.0 and 1.0",
    ):
        selector = SelectColumnsByCorrelation(
            df=df,
            columns_to_keep=["feature_1"],
            target_col_name="target",
            correlation_threshold=-0.1,
        )
        selector.apply()

    # Threshold > 1
    with pytest.raises(
        ValueError,
        match="correlation_threshold must be between 0.0 and 1.0",
    ):
        selector = SelectColumnsByCorrelation(
            df=df,
            columns_to_keep=["feature_1"],
            target_col_name="target",
            correlation_threshold=1.1,
        )
        selector.apply()


def test_target_column_not_numeric_raises():
    """Non-numeric target column -> raises ValueError when building correlation matrix"""
    df = pd.DataFrame(
        {
            "feature_1": [1, 2, 3],
            "target": ["a", "b", "c"],  # non-numeric
        }
    )

    with pytest.raises(
        ValueError,
        match="is not numeric or cannot be used in the correlation matrix",
    ):
        selector = SelectColumnsByCorrelation(
            df=df,
            columns_to_keep=["feature_1"],
            target_col_name="target",
            correlation_threshold=0.5,
        )
        selector.apply()


def test_select_columns_by_correlation_basic():
    """Selects numeric columns above correlation threshold and keeps columns_to_keep"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=5, freq="H"),
            "feature_pos": [1, 2, 3, 4, 5],        # corr = 1.0 with target
            "feature_neg": [5, 4, 3, 2, 1],        # corr = -1.0 with target
            "feature_low": [0, 0, 1, 0, 0],        # low corr with target
            "constant": [10, 10, 10, 10, 10],      # no corr / NaN
            "target": [1, 2, 3, 4, 5],
        }
    )

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["timestamp"],  # should always be kept
        target_col_name="target",
        correlation_threshold=0.8,
    )
    result_df = selector.apply()

    # Expected columns:
    # - "timestamp" from columns_to_keep
    # - "feature_pos" and "feature_neg" due to high absolute correlation
    # - "target" itself (corr=1.0 with itself)
    expected_columns = {"timestamp", "feature_pos", "feature_neg", "target"}

    assert set(result_df.columns) == expected_columns

    # Ensure values of kept columns are identical to original
    pd.testing.assert_series_equal(result_df["feature_pos"], df["feature_pos"])
    pd.testing.assert_series_equal(result_df["feature_neg"], df["feature_neg"])
    pd.testing.assert_series_equal(result_df["target"], df["target"])
    pd.testing.assert_series_equal(result_df["timestamp"], df["timestamp"])


def test_correlation_filter_includes_only_features_above_threshold():
    """Features with high correlation are kept, weakly correlated ones are removed"""
    df = pd.DataFrame(
        {
            "keep_col": ["a", "b", "c", "d", "e"],
            # Strong positive correlation with target
            "feature_strong": [1, 2, 3, 4, 5],
            # Weak correlation / almost noise
            "feature_weak": [0, 1, 0, 1, 0],
            "target": [2, 4, 6, 8, 10],
        }
    )

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["keep_col"],
        target_col_name="target",
        correlation_threshold=0.8,
    )
    result_df = selector.apply()

    # Only strongly correlated features should remain
    assert "keep_col" in result_df.columns
    assert "target" in result_df.columns
    assert "feature_strong" in result_df.columns
    assert "feature_weak" not in result_df.columns


def test_correlation_filter_uses_absolute_value_for_negative_correlation():
    """Features with strong negative correlation are included via absolute correlation"""
    df = pd.DataFrame(
        {
            "keep_col": [0, 1, 2, 3, 4],
            "feature_pos": [1, 2, 3, 4, 5],   # strong positive correlation
            "feature_neg": [5, 4, 3, 2, 1],   # strong negative correlation
            "target": [10, 20, 30, 40, 50],
        }
    )

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["keep_col"],
        target_col_name="target",
        correlation_threshold=0.9,
    )
    result_df = selector.apply()

    # Both positive and negative strongly correlated features should be included
    assert "keep_col" in result_df.columns
    assert "target" in result_df.columns
    assert "feature_pos" in result_df.columns
    assert "feature_neg" in result_df.columns


def test_correlation_threshold_zero_keeps_all_numeric_features():
    """Threshold 0.0 -> all numeric columns are kept regardless of correlation strength"""
    df = pd.DataFrame(
        {
            "keep_col": ["x", "y", "z", "x"],
            "feature_1": [1, 2, 3, 4],           # correlated with target
            "feature_2": [4, 3, 2, 1],           # negatively correlated
            "feature_weak": [0, 1, 0, 1],        # weak correlation
            "target": [10, 20, 30, 40],
        }
    )

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["keep_col"],
        target_col_name="target",
        correlation_threshold=0.0,
    )
    result_df = selector.apply()

    # All numeric columns + keep_col should be present
    expected_columns = {"keep_col", "feature_1", "feature_2", "feature_weak", "target"}
    assert set(result_df.columns) == expected_columns


def test_columns_to_keep_can_be_non_numeric():
    """Non-numeric columns in columns_to_keep are preserved even if not in correlation matrix"""
    df = pd.DataFrame(
        {
            "id": ["a", "b", "c", "d"],
            "category": ["x", "x", "y", "y"],
            "feature_1": [1.0, 2.0, 3.0, 4.0],
            "target": [10.0, 20.0, 30.0, 40.0],
        }
    )

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["id", "category"],
        target_col_name="target",
        correlation_threshold=0.1,
    )
    result_df = selector.apply()

    # id and category must be present regardless of correlation
    assert "id" in result_df.columns
    assert "category" in result_df.columns

    # Numeric feature_1 and target should also be in result due to correlation
    assert "feature_1" in result_df.columns
    assert "target" in result_df.columns


def test_original_dataframe_not_modified_in_place():
    """Ensure the original DataFrame is not modified in place"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=3, freq="H"),
            "feature_1": [1, 2, 3],
            "feature_2": [3, 2, 1],
            "target": [1, 2, 3],
        }
    )

    df_copy = df.copy(deep=True)

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["timestamp"],
        target_col_name="target",
        correlation_threshold=0.9,
    )
    _ = selector.apply()

    # Original DataFrame must be unchanged
    pd.testing.assert_frame_equal(df, df_copy)


def test_no_numeric_columns_except_target_results_in_keep_only():
    """When no other numeric columns besides target exist, result contains only columns_to_keep + target"""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=4, freq="H"),
            "category": ["a", "b", "a", "b"],
            "target": [1, 2, 3, 4],
        }
    )

    selector = SelectColumnsByCorrelation(
        df=df,
        columns_to_keep=["timestamp"],
        target_col_name="target",
        correlation_threshold=0.5,
    )
    result_df = selector.apply()

    expected_columns = {"timestamp", "target"}
    assert set(result_df.columns) == expected_columns


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert SelectColumnsByCorrelation.system_type() == SystemType.PYTHON


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = SelectColumnsByCorrelation.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = SelectColumnsByCorrelation.settings()
    assert isinstance(settings, dict)
    assert settings == {}
