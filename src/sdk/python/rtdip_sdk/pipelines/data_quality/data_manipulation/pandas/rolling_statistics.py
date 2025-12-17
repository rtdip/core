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


# Available statistics that can be computed
AVAILABLE_STATISTICS = ["mean", "std", "min", "max", "sum", "median"]


class RollingStatistics(PandasDataManipulationBaseInterface):
    """
    Computes rolling window statistics for a value column, optionally grouped.

    Rolling statistics capture trends and volatility patterns in time series data.
    Useful for features like moving averages, rolling standard deviation, etc.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.pandas.rolling_statistics import RollingStatistics
    import pandas as pd

    df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=10, freq='D'),
        'group': ['A'] * 5 + ['B'] * 5,
        'value': [10, 20, 30, 40, 50, 100, 200, 300, 400, 500]
    })

    # Compute rolling statistics grouped by 'group'
    roller = RollingStatistics(
        df,
        value_column='value',
        group_columns=['group'],
        windows=[3],
        statistics=['mean', 'std']
    )
    result_df = roller.apply()
    # Result will have columns: date, group, value, rolling_mean_3, rolling_std_3
    ```

    Available statistics: mean, std, min, max, sum, median

    Parameters:
        df (PandasDataFrame): The Pandas DataFrame (should be sorted by time within groups).
        value_column (str): The name of the column to compute statistics from.
        group_columns (List[str], optional): Columns defining separate time series groups.
            If None, statistics are computed across the entire DataFrame.
        windows (List[int], optional): List of window sizes. Defaults to [3, 6, 12].
        statistics (List[str], optional): List of statistics to compute.
            Defaults to ['mean', 'std'].
        min_periods (int, optional): Minimum number of observations required for a result.
            Defaults to 1.
    """

    df: PandasDataFrame
    value_column: str
    group_columns: Optional[List[str]]
    windows: List[int]
    statistics: List[str]
    min_periods: int

    def __init__(
        self,
        df: PandasDataFrame,
        value_column: str,
        group_columns: Optional[List[str]] = None,
        windows: Optional[List[int]] = None,
        statistics: Optional[List[str]] = None,
        min_periods: int = 1,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.group_columns = group_columns
        self.windows = windows if windows is not None else [3, 6, 12]
        self.statistics = statistics if statistics is not None else ["mean", "std"]
        self.min_periods = min_periods

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
        Computes rolling statistics for the specified value column.

        Returns:
            PandasDataFrame: DataFrame with added rolling statistic columns
                (e.g., rolling_mean_3, rolling_std_6).

        Raises:
            ValueError: If the DataFrame is empty, columns don't exist,
                or invalid statistics/windows are specified.
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

        invalid_stats = set(self.statistics) - set(AVAILABLE_STATISTICS)
        if invalid_stats:
            raise ValueError(
                f"Invalid statistics: {invalid_stats}. "
                f"Available: {AVAILABLE_STATISTICS}"
            )

        if not self.windows or any(w <= 0 for w in self.windows):
            raise ValueError("Windows must be a non-empty list of positive integers.")

        result_df = self.df.copy()

        for window in self.windows:
            for stat in self.statistics:
                col_name = f"rolling_{stat}_{window}"

                if self.group_columns:
                    result_df[col_name] = result_df.groupby(self.group_columns)[
                        self.value_column
                    ].transform(
                        lambda x: getattr(
                            x.rolling(window=window, min_periods=self.min_periods), stat
                        )()
                    )
                else:
                    result_df[col_name] = getattr(
                        result_df[self.value_column].rolling(
                            window=window, min_periods=self.min_periods
                        ),
                        stat,
                    )()

        return result_df
