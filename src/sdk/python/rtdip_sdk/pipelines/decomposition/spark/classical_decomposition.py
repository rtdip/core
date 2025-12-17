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


class ClassicalDecomposition(DecompositionBaseInterface):
    """
    Performs classical seasonal decomposition on a PySpark DataFrame.

    Classical decomposition splits a time series into trend, seasonal, and residual components
    using moving averages. Supports both additive and multiplicative models.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.decomposition.spark.classical_decomposition import ClassicalDecomposition
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

    # Example 1: Single time series
    decomposer = ClassicalDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        model='additive',
        period=7
    )
    result = decomposer.decompose()

    # Example 2: Multiple time series (grouped by sensor)
    decomposer_grouped = ClassicalDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        group_columns=['sensor'],
        model='additive',
        period=7
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
        model (str): Type of seasonal component. Must be 'additive' or 'multiplicative'.
        period (int): Periodicity of the seasonal component.
        two_sided (bool): If True, use a centered moving average for trend estimation.
        extrapolate_trend (int or str): How many periods to extrapolate the trend at the boundaries.
    """

    df: PySparkDataFrame
    value_column: str
    timestamp_column: str
    group_columns: List[str]
    model: str
    period: int
    two_sided: bool
    extrapolate_trend: int

    def __init__(
        self,
        df: PySparkDataFrame,
        value_column: str,
        timestamp_column: str = None,
        group_columns: Optional[List[str]] = None,
        model: str = "additive",
        period: int = 7,
        two_sided: bool = True,
        extrapolate_trend: int = 0,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.group_columns = group_columns
        self.model = model
        self.period = period
        self.two_sided = two_sided
        self.extrapolate_trend = extrapolate_trend

        # Validation
        if value_column not in df.columns:
            raise ValueError(f"Column '{value_column}' not found in DataFrame")
        if timestamp_column and timestamp_column not in df.columns:
            raise ValueError(f"Column '{timestamp_column}' not found in DataFrame")
        if group_columns:
            missing_cols = [col for col in group_columns if col not in df.columns]
            if missing_cols:
                raise ValueError(f"Group columns {missing_cols} not found in DataFrame")
        if model not in ["additive", "multiplicative"]:
            raise ValueError(
                "Invalid model type. Must be 'additive' or 'multiplicative'"
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
        from statsmodels.tsa.seasonal import seasonal_decompose

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

        # Perform classical decomposition
        result = seasonal_decompose(
            series,
            model=self.model,
            period=self.period,
            two_sided=self.two_sided,
            extrapolate_trend=self.extrapolate_trend,
        )

        # Add decomposition results to dataframe
        group_pdf = group_pdf.copy()
        group_pdf["trend"] = result.trend.values
        group_pdf["seasonal"] = result.seasonal.values
        group_pdf["residual"] = result.resid.values

        return group_pdf

    def decompose(self) -> PySparkDataFrame:
        """
        Performs classical decomposition on the time series.

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
