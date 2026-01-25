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

import pandas as pd
from pandas import DataFrame as PandasDataFrame
from typing import List, Optional
from ..interfaces import PandasDataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class LagFeatures(PandasDataManipulationBaseInterface):
    """
    Creates lag features from a value column, optionally grouped by specified columns.

    Lag features are essential for time series forecasting with models like XGBoost
    that cannot inherently look back in time. Each lag feature contains the value
    from N periods ago.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.lag_features import LagFeatures
    import pandas as pd

    df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=6, freq='D'),
        'group': ['A', 'A', 'A', 'B', 'B', 'B'],
        'value': [10, 20, 30, 100, 200, 300]
    })

    # Create lag features grouped by 'group'
    lag_creator = LagFeatures(
        df,
        value_column='value',
        group_columns=['group'],
        lags=[1, 2]
    )
    result_df = lag_creator.apply()
    # Result will have columns: date, group, value, lag_1, lag_2
    ```

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame (should be sorted by time within groups).
        value_column (str): The name of the column to create lags from.
        group_columns (List[str], optional): Columns defining separate time series groups.
            If None, lags are computed across the entire DataFrame.
        lags (List[int], optional): List of lag periods. Defaults to [1, 2, 3].
        prefix (str, optional): Prefix for lag column names. Defaults to "lag".
    """

    df: PandasDataFrame
    value_column: str
    group_columns: Optional[List[str]]
    lags: List[int]
    prefix: str

    def __init__(
        self,
        df: PandasDataFrame,
        value_column: str,
        group_columns: Optional[List[str]] = None,
        lags: Optional[List[int]] = None,
        prefix: str = "lag",
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.group_columns = group_columns
        self.lags = lags if lags is not None else [1, 2, 3]
        self.prefix = prefix

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PANDAS
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def apply(self) -> PandasDataFrame:
        """
        Creates lag features for the specified value column.

        Returns:
            PandasDataFrame: DataFrame with added lag columns (lag_1, lag_2, etc.).

        Raises:
            ValueError: If the DataFrame is empty, columns don't exist, or lags are invalid.
        """
        if self.df is None or self.df.empty:
            raise ValueError("The DataFrame is empty.")

        if self.value_column not in self.df.columns:
            raise ValueError(
                f"Column '{self.value_column}' does not exist in the DataFrame."
            )

        if self.group_columns:
            for col in self.group_columns:
                if col not in self.df.columns:
                    raise ValueError(
                        f"Group column '{col}' does not exist in the DataFrame."
                    )

        if not self.lags or any(lag <= 0 for lag in self.lags):
            raise ValueError("Lags must be a non-empty list of positive integers.")

        result_df = self.df.copy()

        for lag in self.lags:
            col_name = f"{self.prefix}_{lag}"

            if self.group_columns:
                result_df[col_name] = result_df.groupby(self.group_columns)[
                    self.value_column
                ].shift(lag)
            else:
                result_df[col_name] = result_df[self.value_column].shift(lag)

        return result_df
