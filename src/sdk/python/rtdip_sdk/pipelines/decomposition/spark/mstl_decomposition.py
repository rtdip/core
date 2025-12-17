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

from typing import Optional, List
from pyspark.sql import DataFrame as PySparkDataFrame
import pandas as pd

from ..interfaces import DecompositionBaseInterface
from ..._pipeline_utils.models import Libraries, SystemType


class MSTLDecomposition(DecompositionBaseInterface):
    """
    Performs MSTL (Multiple Seasonal-Trend decomposition using Loess) on a PySpark DataFrame.

    MSTL extends STL to handle time series with multiple seasonal patterns (e.g., hourly and daily patterns).

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.decomposition.spark.mstl_decomposition import MSTLDecomposition
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Example 1: Single time series
    decomposer = MSTLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        periods=[24, 168]  # Daily and weekly patterns
    )
    result = decomposer.decompose()

    # Example 2: Multiple time series (grouped by sensor)
    decomposer_grouped = MSTLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        group_columns=['sensor'],
        periods=[24, 168]
    )
    result_grouped = decomposer_grouped.decompose()
    ```

    Parameters:
        df (PySparkDataFrame): PySpark DataFrame containing the time series data.
        value_column (str): Name of the column containing the values to decompose.
        timestamp_column (str): Name of the column containing timestamps. If None, assumes ordered data.
        group_columns (List[str], optional): Columns defining separate time series groups (e.g., ['sensor_id']).
            If provided, decomposition is performed separately for each group.
            If None, the entire DataFrame is treated as a single time series.
        periods (int or list): Periodicities of the seasonal components (e.g., [24, 168] for hourly data with daily and weekly patterns).
        windows (int or list, optional): Window sizes for seasonal smoothers. Must match length of periods if provided.
        iterate (int): Number of iterations for the MSTL algorithm.
        stl_kwargs (dict, optional): Additional keyword arguments to pass to the internal STL calls.
    """

    df: PySparkDataFrame
    value_column: str
    timestamp_column: str
    group_columns: List[str]
    periods: list
    windows: list
    iterate: int
    stl_kwargs: dict

    def __init__(
        self,
        df: PySparkDataFrame,
        value_column: str,
        timestamp_column: str = None,
        group_columns: Optional[List[str]] = None,
        periods: int = None,
        windows: int = None,
        iterate: int = 2,
        stl_kwargs: dict = None,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.group_columns = group_columns
        self.periods = (
            periods if isinstance(periods, list) else [periods] if periods else [7]
        )
        self.windows = (
            windows if isinstance(windows, list) else [windows] if windows else None
        )
        self.iterate = iterate
        self.stl_kwargs = stl_kwargs or {}

        # Validation
        if value_column not in df.columns:
            raise ValueError(f"Column '{value_column}' not found in DataFrame")
        if timestamp_column and timestamp_column not in df.columns:
            raise ValueError(f"Column '{timestamp_column}' not found in DataFrame")
        if group_columns:
            missing_cols = [col for col in group_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Group columns {missing_cols} not found in DataFrame")
        if self.windows is not None and len(self.windows) != len(self.periods):
            raise ValueError(
                f"Length of windows ({len(self.windows)}) must match length of periods ({len(self.periods)})"
            )

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

    def _decompose_single_group(self, group_pdf: pd.DataFrame) -> pd.DataFrame:
        """
        Decompose a single group (or the entire DataFrame if no grouping).

        Parameters
        ----------
        group_pdf : pd.DataFrame
            Pandas DataFrame for a single group

        Returns
        -------
        pd.DataFrame
            DataFrame with decomposition components added
        """
        from statsmodels.tsa.seasonal import MSTL

        # Validate group size
        max_period = max(self.periods)
        if len(group_pdf) < 2 * max_period:
            raise ValueError(
                f"Group has {len(group_pdf)} observations, but needs at least "
                f"{2 * max_period} (2 * max_period) for decomposition"
            )

        # Sort by timestamp if provided
        if self.timestamp_column:
            group_pdf = group_pdf.sort_values(self.timestamp_column)

        # Get the series
        series = group_pdf[self.value_column]

        # Validate data
        if series.isna().any():
            raise ValueError(
                f"Time series contains NaN values in column '{self.value_column}'"
            )

        # Perform MSTL decomposition
        mstl = MSTL(
            series,
            periods=self.periods,
            windows=self.windows,
            iterate=self.iterate,
            stl_kwargs=self.stl_kwargs,
        )
        result = mstl.fit()

        # Add decomposition results to dataframe
        group_pdf = group_pdf.copy()
        group_pdf["trend"] = result.trend.values

        # Handle seasonal components (can be Series or DataFrame)
        if len(self.periods) == 1:
            seasonal_col = f"seasonal_{self.periods[0]}"
            group_pdf[seasonal_col] = result.seasonal.values
        else:
            for i, period in enumerate(self.periods):
                seasonal_col = f"seasonal_{period}"
                group_pdf[seasonal_col] = result.seasonal[result.seasonal.columns[i]].values

        group_pdf["residual"] = result.resid.values

        return group_pdf

    def decompose(self) -> PySparkDataFrame:
        """
        Performs MSTL decomposition on the time series.

        If group_columns is provided, decomposition is performed separately for each group.
        Each group must have at least 2 * max_period observations.

        Returns:
            PySparkDataFrame: DataFrame with original columns plus 'trend', 'seasonal_X' (for each period X), and 'residual' columns.

        Raises:
            ValueError: If any group has insufficient data or contains NaN values
        """
        # Convert to pandas
        pdf = self.df.toPandas()

        if self.group_columns:
            # Group by specified columns and decompose each group
            result_dfs = []

            for group_vals, group_df in pdf.groupby(self.group_columns):
                try:
                    decomposed_group = self._decompose_single_group(group_df)
                    result_dfs.append(decomposed_group)
                except ValueError as e:
                    group_str = dict(zip(self.group_columns, group_vals if isinstance(group_vals, tuple) else [group_vals]))
                    raise ValueError(f"Error in group {group_str}: {str(e)}")

            result_pdf = pd.concat(result_dfs, ignore_index=True)
        else:
            # No grouping - decompose entire DataFrame
            result_pdf = self._decompose_single_group(pdf)

        # Convert back to PySpark DataFrame
        result_df = self.df.sql_ctx.createDataFrame(result_pdf)

        return result_df
