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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from typing import List, Optional
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


# Available statistics that can be computed
AVAILABLE_STATISTICS = ["mean", "std", "min", "max", "sum", "median"]


class RollingStatistics(DataManipulationBaseInterface):
    """
    Computes rolling window statistics for a value column, optionally grouped.

    Rolling statistics capture trends and volatility patterns in time series data.
    Useful for features like moving averages, rolling standard deviation, etc.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.rolling_statistics import RollingStatistics
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ('2024-01-01', 'A', 10),
        ('2024-01-02', 'A', 20),
        ('2024-01-03', 'A', 30),
        ('2024-01-04', 'A', 40),
        ('2024-01-05', 'A', 50)
    ], ['date', 'group', 'value'])

    # Compute rolling statistics grouped by 'group'
    roller = RollingStatistics(
        df,
        value_column='value',
        group_columns=['group'],
        windows=[3],
        statistics=['mean', 'std'],
        order_by_columns=['date']
    )
    result_df = roller.filter_data()
    # Result will have columns: date, group, value, rolling_mean_3, rolling_std_3
    ```

    Available statistics: mean, std, min, max, sum, median

    Parameters:
        df (DataFrame): The PySpark DataFrame.
        value_column (str): The name of the column to compute statistics from.
        group_columns (List[str], optional): Columns defining separate time series groups.
            If None, statistics are computed across the entire DataFrame.
        windows (List[int], optional): List of window sizes. Defaults to [3, 6, 12].
        statistics (List[str], optional): List of statistics to compute.
            Defaults to ['mean', 'std'].
        order_by_columns (List[str], optional): Columns to order by within groups.
            If None, uses the natural order of the DataFrame.
    """

    df: DataFrame
    value_column: str
    group_columns: Optional[List[str]]
    windows: List[int]
    statistics: List[str]
    order_by_columns: Optional[List[str]]

    def __init__(
        self,
        df: DataFrame,
        value_column: str,
        group_columns: Optional[List[str]] = None,
        windows: Optional[List[int]] = None,
        statistics: Optional[List[str]] = None,
        order_by_columns: Optional[List[str]] = None,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.group_columns = group_columns
        self.windows = windows if windows is not None else [3, 6, 12]
        self.statistics = statistics if statistics is not None else ["mean", "std"]
        self.order_by_columns = order_by_columns

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def filter_data(self) -> DataFrame:
        """
        Computes rolling statistics for the specified value column.

        Returns:
            DataFrame: DataFrame with added rolling statistic columns
                (e.g., rolling_mean_3, rolling_std_6).

        Raises:
            ValueError: If the DataFrame is None, columns don't exist,
                or invalid statistics/windows are specified.
        """
        if self.df is None:
            raise ValueError("The DataFrame is None.")

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

        if self.order_by_columns:
            for col in self.order_by_columns:
                if col not in self.df.columns:
                    raise ValueError(
                        f"Order by column '{col}' does not exist in the DataFrame."
                    )

        invalid_stats = set(self.statistics) - set(AVAILABLE_STATISTICS)
        if invalid_stats:
            raise ValueError(
                f"Invalid statistics: {invalid_stats}. "
                f"Available: {AVAILABLE_STATISTICS}"
            )

        if not self.windows or any(w <= 0 for w in self.windows):
            raise ValueError("Windows must be a non-empty list of positive integers.")

        result_df = self.df

        # Define window specification
        if self.group_columns and self.order_by_columns:
            base_window = Window.partitionBy(
                [F.col(c) for c in self.group_columns]
            ).orderBy([F.col(c) for c in self.order_by_columns])
        elif self.group_columns:
            base_window = Window.partitionBy([F.col(c) for c in self.group_columns])
        elif self.order_by_columns:
            base_window = Window.orderBy([F.col(c) for c in self.order_by_columns])
        else:
            base_window = Window.orderBy(F.monotonically_increasing_id())

        # Compute rolling statistics
        for window_size in self.windows:
            # Define rolling window with row-based window frame
            rolling_window = base_window.rowsBetween(-(window_size - 1), 0)

            for stat in self.statistics:
                col_name = f"rolling_{stat}_{window_size}"

                if stat == "mean":
                    result_df = result_df.withColumn(
                        col_name, F.avg(F.col(self.value_column)).over(rolling_window)
                    )
                elif stat == "std":
                    result_df = result_df.withColumn(
                        col_name,
                        F.stddev(F.col(self.value_column)).over(rolling_window),
                    )
                elif stat == "min":
                    result_df = result_df.withColumn(
                        col_name, F.min(F.col(self.value_column)).over(rolling_window)
                    )
                elif stat == "max":
                    result_df = result_df.withColumn(
                        col_name, F.max(F.col(self.value_column)).over(rolling_window)
                    )
                elif stat == "sum":
                    result_df = result_df.withColumn(
                        col_name, F.sum(F.col(self.value_column)).over(rolling_window)
                    )
                elif stat == "median":
                    # Median requires percentile_approx in window function
                    result_df = result_df.withColumn(
                        col_name,
                        F.expr(
                            f"percentile_approx({self.value_column}, 0.5)"
                        ).over(rolling_window),
                    )

        return result_df
