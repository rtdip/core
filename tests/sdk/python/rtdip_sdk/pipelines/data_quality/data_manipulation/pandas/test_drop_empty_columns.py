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

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.drop_empty_columns import (
    DropEmptyAndUselessColumns,
)


def test_empty_df():
    """Empty DataFrame"""
    empty_df = pd.DataFrame()

    with pytest.raises(ValueError, match="The DataFrame is empty."):
        cleaner = DropEmptyAndUselessColumns(empty_df)
        cleaner.apply()


def test_none_df():
    """DataFrame is None"""
    with pytest.raises(ValueError, match="The DataFrame is empty."):
        cleaner = DropEmptyAndUselessColumns(None)
        cleaner.apply()


def test_drop_empty_and_constant_columns():
    """Drops fully empty and constant columns"""
    data = {
        "a": [1, 2, 3],                 # informative
        "b": [np.nan, np.nan, np.nan],  # all NaN -> drop
        "c": [5, 5, 5],                 # constant -> drop
        "d": [np.nan, 7, 7],            # non-NaN all equal -> drop
        "e": [1, np.nan, 2],            # at least 2 unique non-NaN -> keep
    }
    df = pd.DataFrame(data)

    cleaner = DropEmptyAndUselessColumns(df)
    result_df = cleaner.apply()

    # Expected kept columns
    assert list(result_df.columns) == ["a", "e"]

    # Check values preserved for kept columns
    pd.testing.assert_series_equal(result_df["a"], df["a"])
    pd.testing.assert_series_equal(result_df["e"], df["e"])


def test_mostly_nan_but_multiple_unique_values_kept():
    """Keeps column with multiple unique non-NaN values even if many NaNs"""
    data = {
        "a": [np.nan, 1, np.nan, 2, np.nan],  # two unique non-NaN -> keep
        "b": [np.nan, np.nan, np.nan, np.nan, np.nan],  # all NaN -> drop
    }
    df = pd.DataFrame(data)

    cleaner = DropEmptyAndUselessColumns(df)
    result_df = cleaner.apply()

    assert "a" in result_df.columns
    assert "b" not in result_df.columns
    assert result_df["a"].nunique(dropna=True) == 2


def test_no_columns_to_drop_returns_same_columns():
    """No empty or constant columns -> DataFrame unchanged (column-wise)"""
    data = {
        "a": [1, 2, 3],
        "b": [1.0, 1.5, 2.0],
        "c": ["x", "y", "z"],
    }
    df = pd.DataFrame(data)

    cleaner = DropEmptyAndUselessColumns(df)
    result_df = cleaner.apply()

    assert list(result_df.columns) == list(df.columns)
    pd.testing.assert_frame_equal(result_df, df)


def test_original_dataframe_not_modified_in_place():
    """Ensure the original DataFrame is not modified in place"""
    data = {
        "a": [1, 2, 3],
        "b": [np.nan, np.nan, np.nan],  # will be dropped in result
    }
    df = pd.DataFrame(data)

    cleaner = DropEmptyAndUselessColumns(df)
    result_df = cleaner.apply()

    # Original DataFrame still has both columns
    assert list(df.columns) == ["a", "b"]

    # Result DataFrame has only the informative column
    assert list(result_df.columns) == ["a"]
