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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.mad_outlier_detection import (
    MADOutlierDetection,
)


def test_empty_df():
    """Empty DataFrame"""
    empty_df = pd.DataFrame(columns=["TagName", "Value"])

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        detector = MADOutlierDetection(empty_df, "Value")
        detector.apply()


def test_column_not_exists():
    """Column does not exist"""
    data = {
        "TagName": ["Tag_A", "Tag_B"],
        "Value": [1.0, 2.0],
    }
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Column 'NonExistent' does not exist"):
        detector = MADOutlierDetection(df, "NonExistent")
        detector.apply()


def test_invalid_action():
    """Invalid action parameter"""
    data = {"Value": [1.0, 2.0, 3.0]}
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="Invalid action"):
        detector = MADOutlierDetection(df, "Value", action="invalid")
        detector.apply()


def test_invalid_n_sigma():
    """Invalid n_sigma parameter"""
    data = {"Value": [1.0, 2.0, 3.0]}
    df = pd.DataFrame(data)

    with pytest.raises(ValueError, match="n_sigma must be positive"):
        detector = MADOutlierDetection(df, "Value", n_sigma=-1)
        detector.apply()


def test_flag_action_detects_outlier():
    """Flag action correctly identifies outliers"""
    data = {"Value": [10.0, 11.0, 12.0, 10.5, 11.5, 1000000.0]}  # Last value is outlier
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", n_sigma=3.0, action="flag")
    result_df = detector.apply()

    assert "Value_is_outlier" in result_df.columns
    # The extreme value should be flagged
    assert result_df["Value_is_outlier"].iloc[-1] == True
    # Normal values should not be flagged
    assert result_df["Value_is_outlier"].iloc[0] == False


def test_flag_action_custom_column_name():
    """Flag action with custom outlier column name"""
    data = {"Value": [10.0, 11.0, 1000000.0]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(
        df, "Value", action="flag", outlier_column="is_extreme"
    )
    result_df = detector.apply()

    assert "is_extreme" in result_df.columns
    assert "Value_is_outlier" not in result_df.columns


def test_replace_action():
    """Replace action replaces outliers with specified value"""
    data = {"Value": [10.0, 11.0, 12.0, 1000000.0]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(
        df, "Value", n_sigma=3.0, action="replace", replacement_value=-1
    )
    result_df = detector.apply()

    assert result_df["Value"].iloc[-1] == -1
    assert result_df["Value"].iloc[0] == 10.0


def test_replace_action_default_nan():
    """Replace action uses NaN when no replacement value specified"""
    data = {"Value": [10.0, 11.0, 12.0, 1000000.0]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", n_sigma=3.0, action="replace")
    result_df = detector.apply()

    assert pd.isna(result_df["Value"].iloc[-1])


def test_remove_action():
    """Remove action removes rows with outliers"""
    data = {"TagName": ["A", "B", "C", "D"], "Value": [10.0, 11.0, 12.0, 1000000.0]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", n_sigma=3.0, action="remove")
    result_df = detector.apply()

    assert len(result_df) == 3
    assert 1000000.0 not in result_df["Value"].values


def test_exclude_values():
    """Excluded values are not considered in MAD calculation"""
    data = {"Value": [10.0, 11.0, 12.0, -1, -1, 1000000.0]}  # -1 are error codes
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(
        df, "Value", n_sigma=3.0, action="flag", exclude_values=[-1]
    )
    result_df = detector.apply()

    # Error codes should not be flagged as outliers
    assert result_df["Value_is_outlier"].iloc[3] == False
    assert result_df["Value_is_outlier"].iloc[4] == False
    # Extreme value should still be flagged
    assert result_df["Value_is_outlier"].iloc[-1] == True


def test_no_outliers():
    """No outliers in data"""
    data = {"Value": [10.0, 10.5, 11.0, 10.2, 10.8]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", n_sigma=3.0, action="flag")
    result_df = detector.apply()

    assert not result_df["Value_is_outlier"].any()


def test_all_same_values():
    """All values are the same (MAD = 0)"""
    data = {"Value": [10.0, 10.0, 10.0, 10.0]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", n_sigma=3.0, action="flag")
    result_df = detector.apply()

    # With MAD = 0, bounds = median ± 0, so any value equal to median is not an outlier
    assert not result_df["Value_is_outlier"].any()


def test_negative_outliers():
    """Detects negative outliers"""
    data = {"Value": [10.0, 11.0, 12.0, 10.5, -1000000.0]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", n_sigma=3.0, action="flag")
    result_df = detector.apply()

    assert result_df["Value_is_outlier"].iloc[-1] == True


def test_both_direction_outliers():
    """Detects outliers in both directions"""
    data = {"Value": [-1000000.0, 10.0, 11.0, 12.0, 1000000.0]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", n_sigma=3.0, action="flag")
    result_df = detector.apply()

    assert result_df["Value_is_outlier"].iloc[0] == True
    assert result_df["Value_is_outlier"].iloc[-1] == True


def test_preserves_other_columns():
    """Ensures other columns are preserved"""
    data = {
        "TagName": ["A", "B", "C", "D"],
        "EventTime": ["2024-01-01", "2024-01-02", "2024-01-03", "2024-01-04"],
        "Value": [10.0, 11.0, 12.0, 1000000.0],
    }
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", action="flag")
    result_df = detector.apply()

    assert "TagName" in result_df.columns
    assert "EventTime" in result_df.columns
    assert list(result_df["TagName"]) == ["A", "B", "C", "D"]


def test_does_not_modify_original():
    """Ensures original DataFrame is not modified"""
    data = {"Value": [10.0, 11.0, 1000000.0]}
    df = pd.DataFrame(data)
    original_df = df.copy()

    detector = MADOutlierDetection(df, "Value", action="replace", replacement_value=-1)
    result_df = detector.apply()

    pd.testing.assert_frame_equal(df, original_df)


def test_with_nan_values():
    """NaN values are excluded from MAD calculation"""
    data = {"Value": [10.0, 11.0, np.nan, 12.0, 1000000.0]}
    df = pd.DataFrame(data)
    detector = MADOutlierDetection(df, "Value", n_sigma=3.0, action="flag")
    result_df = detector.apply()

    # NaN should not be flagged as outlier
    assert result_df["Value_is_outlier"].iloc[2] == False
    # Extreme value should be flagged
    assert result_df["Value_is_outlier"].iloc[-1] == True


def test_different_n_sigma_values():
    """Different n_sigma values affect outlier detection"""
    data = {"Value": [10.0, 11.0, 12.0, 13.0, 20.0]}  # 20.0 is mildly extreme
    df = pd.DataFrame(data)

    # With low n_sigma, 20.0 should be flagged
    detector_strict = MADOutlierDetection(df, "Value", n_sigma=1.0, action="flag")
    result_strict = detector_strict.apply()

    # With high n_sigma, 20.0 might not be flagged
    detector_loose = MADOutlierDetection(df, "Value", n_sigma=10.0, action="flag")
    result_loose = detector_loose.apply()

    # Strict should flag more or equal outliers than loose
    assert (
        result_strict["Value_is_outlier"].sum()
        >= result_loose["Value_is_outlier"].sum()
    )
