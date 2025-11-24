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

from pandas import DataFrame as PandasDataFrame
from ..interfaces import PandasDataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class DropEmptyColumns(PandasDataManipulationBaseInterface):
    """
    Removes columns that do not contain any non-NaN values.

    This component scans all DataFrame columns and identifies those where every
    entry is either missing (NaN) or the column contains zero unique values.
    Such columns are typically either empty placeholders or improperly loaded
    fields from upstream data sources.

    The transformation returns a cleaned DataFrame with only columns that
    contain at least one valid (non-NaN) entry.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.drop_empty_columns import DropEmptyColumns
    import pandas as pd

    df = pd.DataFrame({
        'a': [1, 2, 3],
        'b': [None, None, None],       # Empty column
        'c': [5, None, 7],
        'd': [NaN, NaN, NaN]           # Empty column
    })

    cleaner = DropEmptyColumns(df)
    result_df = cleaner.apply()

    # result_df:
    #    a    c
    # 0  1  5.0
    # 1  2  NaN
    # 2  3  7.0
    ```

    Parameters
    ----------
    df : PandasDataFrame
        The Pandas DataFrame whose columns should be examined and cleaned.
    """

    df: PandasDataFrame

    def __init__(
            self,
            df: PandasDataFrame,
    ) -> None:
        self.df = df

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PANDAS
        """
        return SystemType.PANDAS

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def apply(self) -> PandasDataFrame:
        """
        Removes columns without values other than NaN from the DataFrame

        Returns:
            PandasDataFrame: DataFrame without empty columns

        Raises:
            ValueError: If the DataFrame is empty or column doesn't exist.
        """

        # Ensure DataFrame is present and contains rows
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        # Count unique non-NaN values per column
        n_unique = self.df.nunique(dropna=True)

        # Identify columns with zero non-null unique values -> empty columns
        cols_to_drop = n_unique[n_unique == 0].index.tolist()

        # Create cleaned DataFrame without empty columns
        result_df = self.df.copy()
        result_df = result_df.drop(columns=cols_to_drop)

        return result_df
