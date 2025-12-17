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


class STLDecomposition(DecompositionBaseInterface):
    """
    Performs STL (Seasonal and Trend decomposition using Loess) on a PySpark DataFrame.

    STL is a robust method for decomposing time series into trend, seasonal, and residual components.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.decomposition.spark.stl_decomposition import STLDecomposition
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Example 1: Single time series
    decomposer = STLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        period=7
    )
    result = decomposer.decompose()

    # Example 2: Multiple time series (grouped by sensor)
    decomposer_grouped = STLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        group_columns=['sensor'],
        period=7,
        robust=True
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
        period (int): Periodicity of the seasonal component (e.g., 7 for weekly, 24 for hourly daily pattern).
        seasonal (int, optional): Length of the seasonal smoother. Must be odd.
        trend (int, optional): Length of the trend smoother. Must be odd.
        robust (bool): If True, use robust weights in the fitting procedure.
    """

    df: PySparkDataFrame
    value_column: str
    timestamp_column: str
    group_columns: List[str]
    period: int
    seasonal: int
    trend: int
    robust: bool

    def __init__(
        self,
        df: PySparkDataFrame,
        value_column: str,
        timestamp_column: str = None,
        group_columns: Optional[List[str]] = None,
        period: int = 7,
        seasonal: int = None,
        trend: int = None,
        robust: bool = False,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.group_columns = group_columns
        self.period = period
        self.seasonal = seasonal
        self.trend = trend
        self.robust = robust

        # Validation
        if value_column not in df.columns:
            raise ValueError(f"Column '{value_column}' not found in DataFrame")
        if timestamp_column and timestamp_column not in df.columns:
            raise ValueError(f"Column '{timestamp_column}' not found in DataFrame")
        if group_columns:
            missing_cols = [col for col in group_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Group columns {missing_cols} not found in DataFrame")

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
        from statsmodels.tsa.seasonal import STL

        # Validate group size
        if len(group_pdf) < 2 * self.period:
            raise ValueError(
                f"Group has {len(group_pdf)} observations, but needs at least "
                f"{2 * self.period} (2 * period) for decomposition"
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

        # Set default seasonal smoother length if not provided
        seasonal = self.seasonal
        if seasonal is None:
            seasonal = self.period + 1 if self.period % 2 == 0 else self.period

        # Perform STL decomposition
        stl = STL(
            series,
            period=self.period,
            seasonal=seasonal,
            trend=self.trend,
            robust=self.robust,
        )
        result = stl.fit()

        # Add decomposition results to dataframe
        group_pdf = group_pdf.copy()
        group_pdf["trend"] = result.trend.values
        group_pdf["seasonal"] = result.seasonal.values
        group_pdf["residual"] = result.resid.values

        return group_pdf

    def decompose(self) -> PySparkDataFrame:
        """
        Performs STL decomposition on the time series.

        If group_columns is provided, decomposition is performed separately for each group.
        Each group must have at least 2 * period observations.

        Returns:
            PySparkDataFrame: DataFrame with original columns plus 'trend', 'seasonal', and 'residual' columns.

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
