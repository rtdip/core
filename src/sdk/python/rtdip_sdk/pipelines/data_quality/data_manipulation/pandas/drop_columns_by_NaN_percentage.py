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


class DropByNaNPercentage(PandasDataManipulationBaseInterface):
    """
    Drops all DataFrame columns whose percentage of NaN values exceeds
    a user-defined threshold.

    This transformation is useful when working with wide datasets that contain
    many partially populated or sparsely filled columns. Columns with too many
    missing values tend to carry little predictive value and may negatively
    affect downstream analytics or machine learning tasks.

    The component analyzes each column, computes its NaN ratio, and removes
    any column where the ratio exceeds the configured threshold.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.drop_by_nan_percentage import DropByNaNPercentage
    import pandas as pd

    df = pd.DataFrame({
        'a': [1, None, 3],         # 33% NaN
        'b': [None, None, None],   # 100% NaN
        'c': [7, 8, 9],            # 0% NaN
        'd': [1, None, None],      # 66% NaN
    })

    dropper = DropByNaNPercentage(df, nan_threshold=0.5)
    cleaned_df = dropper.apply()

    # cleaned_df:
    #    a  c
    # 0  1  7
    # 1 NaN 8
    # 2  3  9
    ```

    Parameters
    ----------
    df : PandasDataFrame
        The input DataFrame from which columns should be removed.
    nan_threshold : float
        Threshold between 0 and 1 indicating the minimum NaN ratio at which
        a column should be dropped (e.g., 0.3 = 30% or more NaN).
    """

    df: PandasDataFrame
    nan_threshold: float

    def __init__(
            self,
            df: PandasDataFrame,
            nan_threshold
    ) -> None:
        self.df = df
        self.nan_threshold = nan_threshold

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

        if self.nan_threshold < 0:
            raise ValueError("NaN Threshold is negative.")

        # Create cleaned DataFrame without empty columns
        result_df = self.df.copy()

        if self.nan_threshold == 0.0:
            cols_to_drop = result_df.columns[result_df.isna().any()].tolist()
        else:

            row_count = len(self.df.index)
            nan_ratio = self.df.isna().sum() / row_count
            cols_to_drop = nan_ratio[nan_ratio >= self.nan_threshold].index.tolist()

        result_df = result_df.drop(columns=cols_to_drop)

        return result_df
