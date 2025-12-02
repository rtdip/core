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

from typing import Optional, List, Union
from pyspark.sql import DataFrame as PySparkDataFrame
import pandas as pd

from ..interfaces import DecompositionBaseInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..pandas.period_utils import calculate_period_from_frequency


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

    # Example 1: Single time series with explicit periods
    decomposer = MSTLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        periods=[24, 168]  # Daily and weekly patterns (explicit)
    )
    result = decomposer.decompose()

    # Or using period strings (auto-calculated from sampling frequency)
    decomposer = MSTLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        periods=['daily', 'weekly']  # Automatically calculated
    )
    result = decomposer.decompose()

    # Example 2: Multiple time series (grouped by sensor)
    decomposer_grouped = MSTLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        group_columns=['sensor'],
        periods=['daily', 'weekly']  # Period strings
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
        periods (Union[int, List[int], str, List[str]]): Seasonal period(s). Can be:
            - Integer(s): Explicit period values (e.g., 7 for weekly, [24, 168])
            - String(s): Period names that are auto-calculated from sampling frequency
              Supported: 'minutely', 'hourly', 'daily', 'weekly', 'monthly',
              'quarterly', 'yearly'
            Examples:
            - [24, 168] for daily+weekly in hourly data (explicit)
            - ['hourly', 'daily'] for auto-calculated periods based on sampling
            - ['daily', 'weekly'] for daily data with weekly+yearly patterns
        windows (int or list, optional): Window sizes for seasonal smoothers. Must match length of periods if provided.
        iterate (int): Number of iterations for the MSTL algorithm.
        stl_kwargs (dict, optional): Additional keyword arguments to pass to the internal STL calls.
    """

    df: PySparkDataFrame
    value_column: str
    timestamp_column: str
    group_columns: List[str]
    periods_input: Union[int, List[int], str, List[str]]
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
        periods: Union[int, List[int], str, List[str]] = None,
        windows: int = None,
        iterate: int = 2,
        stl_kwargs: dict = None,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.group_columns = group_columns
        self.periods_input = periods if periods else [7]  # Store original input
        self.periods = None  # Will be resolved in _resolve_periods
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

    def _resolve_periods(self, group_pdf: pd.DataFrame) -> List[int]:
        """
        Resolve period specifications (strings or integers) to integer values.

        Parameters
        ----------
        group_pdf : pd.DataFrame
            Pandas DataFrame for the group (needed to calculate periods from frequency)

        Returns
        -------
        List[int]
            List of resolved period values
        """
        # Convert to list if single value
        periods_input = (
            self.periods_input
            if isinstance(self.periods_input, list)
            else [self.periods_input]
        )

        resolved_periods = []

        for period_spec in periods_input:
            if isinstance(period_spec, str):
                # String period name - calculate from sampling frequency
                if not self.timestamp_column:
                    raise ValueError(
                        f"timestamp_column must be provided when using period strings like '{period_spec}'"
                    )

                period = calculate_period_from_frequency(
                    df=group_pdf,
                    timestamp_column=self.timestamp_column,
                    period_name=period_spec,
                    min_cycles=2,
                )

                if period is None:
                    raise ValueError(
                        f"Period '{period_spec}' is not valid for this data. "
                        f"Either the calculated period is too small (<2) or there is insufficient "
                        f"data for at least 2 complete cycles."
                    )

                resolved_periods.append(period)
            elif isinstance(period_spec, int):
                # Integer period - use directly
                if period_spec < 2:
                    raise ValueError(
                        f"All periods must be at least 2, got {period_spec}"
                    )
                resolved_periods.append(period_spec)
            else:
                raise ValueError(
                    f"Period must be int or str, got {type(period_spec).__name__}"
                )

        # Validate length requirement
        max_period = max(resolved_periods)
        if len(group_pdf) < 2 * max_period:
            raise ValueError(
                f"Time series length ({len(group_pdf)}) must be at least "
                f"2 * max_period ({2 * max_period})"
            )

        # Validate windows if provided
        if self.windows is not None:
            windows_list = (
                self.windows if isinstance(self.windows, list) else [self.windows]
            )
            if len(windows_list) != len(resolved_periods):
                raise ValueError(
                    f"Length of windows ({len(windows_list)}) must match length of periods ({len(resolved_periods)})"
                )

        return resolved_periods

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

        # Resolve periods for this group
        resolved_periods = self._resolve_periods(group_pdf)

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
            periods=resolved_periods,
            windows=self.windows,
            iterate=self.iterate,
            stl_kwargs=self.stl_kwargs,
        )
        result = mstl.fit()

        # Add decomposition results to dataframe
        group_pdf = group_pdf.copy()
        group_pdf["trend"] = result.trend.values

        # Handle seasonal components (can be Series or DataFrame)
        if len(resolved_periods) == 1:
            seasonal_col = f"seasonal_{resolved_periods[0]}"
            group_pdf[seasonal_col] = result.seasonal.values
        else:
            for i, period in enumerate(resolved_periods):
                seasonal_col = f"seasonal_{period}"
                group_pdf[seasonal_col] = result.seasonal[
                    result.seasonal.columns[i]
                ].values

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
                    group_str = dict(
                        zip(
                            self.group_columns,
                            (
                                group_vals
                                if isinstance(group_vals, tuple)
                                else [group_vals]
                            ),
                        )
                    )
                    raise ValueError(f"Error in group {group_str}: {str(e)}")

            result_pdf = pd.concat(result_dfs, ignore_index=True)
        else:
            # No grouping - decompose entire DataFrame
            result_pdf = self._decompose_single_group(pdf)

        # Convert back to PySpark DataFrame
        result_df = self.df.sql_ctx.createDataFrame(result_pdf)

        return result_df
