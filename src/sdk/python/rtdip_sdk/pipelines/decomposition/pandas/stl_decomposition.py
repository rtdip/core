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
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from statsmodels.tsa.seasonal import STL

from ..interfaces import PandasDecompositionBaseInterface
from ..._pipeline_utils.models import Libraries, SystemType


class STLDecomposition(PandasDecompositionBaseInterface):
    """
    Decomposes a time series using STL (Seasonal and Trend decomposition using Loess).

    STL is a robust and flexible method for decomposing time series. It uses locally
    weighted regression (LOESS) for smooth trend estimation and can handle outliers
    through iterative weighting. The seasonal component is allowed to change over time.

    Example
    -------
    ```python
    import pandas as pd
    import numpy as np
    from rtdip_sdk.pipelines.decomposition.pandas import STLDecomposition

    # Example 1: Single time series
    dates = pd.date_range('2024-01-01', periods=365, freq='D')
    df = pd.DataFrame({
        'timestamp': dates,
        'value': np.sin(np.arange(365) * 2 * np.pi / 7) + np.arange(365) * 0.01 + np.random.randn(365) * 0.1
    })

    decomposer = STLDecomposition(
        df=df,
        value_column='value',
        timestamp_column='timestamp',
        period=7,
        robust=True
    )
    result_df = decomposer.decompose()

    # Example 2: Multiple time series (grouped by sensor)
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    df_multi = pd.DataFrame({
        'timestamp': dates.tolist() * 3,
        'sensor': ['A'] * 100 + ['B'] * 100 + ['C'] * 100,
        'value': np.random.randn(300)
    })

    decomposer_grouped = STLDecomposition(
        df=df_multi,
        value_column='value',
        timestamp_column='timestamp',
        group_columns=['sensor'],
        period=7,
        robust=True
    )
    result_df_grouped = decomposer_grouped.decompose()
    ```

    Parameters
    ----------
    df : PandasDataFrame
        Input DataFrame containing the time series data
    value_column : str
        Name of the column containing the values to decompose
    timestamp_column : str, optional
        Name of the column containing timestamps. If provided, will be used
        to set the index. If None, assumes index is already a DatetimeIndex.
    group_columns : List[str], optional
        Columns defining separate time series groups (e.g., ['sensor_id']).
        If provided, decomposition is performed separately for each group.
        If None, the entire DataFrame is treated as a single time series.
    period : int
        Seasonal period (e.g., 7 for weekly, 24 for hourly daily patterns, 365 for yearly)
    seasonal : int, optional
        Length of seasonal smoother (must be odd). If None, defaults to period + 1 if even, else period.
    trend : int, optional
        Length of trend smoother (must be odd). If None, it is estimated from the data.
    robust : bool, default=False
        Whether to use robust weights for outlier handling

    Attributes
    ----------
    result_df : PandasDataFrame
        DataFrame with original data plus decomposed components
    """

    def __init__(
        self,
        df: PandasDataFrame,
        value_column: str,
        timestamp_column: Optional[str] = None,
        group_columns: Optional[List[str]] = None,
        period: int = 7,
        seasonal: Optional[int] = None,
        trend: Optional[int] = None,
        robust: bool = False,
    ):
        self.df = df.copy()
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.group_columns = group_columns
        self.period = period
        self.seasonal = seasonal
        self.trend = trend
        self.robust = robust
        self.result_df = None

        self._validate_inputs()

    def _validate_inputs(self):
        """Validate input parameters."""
        if self.value_column not in self.df.columns:
            raise ValueError(f"Column '{self.value_column}' not found in DataFrame")

        if self.timestamp_column and self.timestamp_column not in self.df.columns:
            raise ValueError(f"Column '{self.timestamp_column}' not found in DataFrame")

        if self.group_columns:
            missing_cols = [col for col in self.group_columns if col not in self.df.columns]
            if missing_cols:
                raise ValueError(f"Group columns {missing_cols} not found in DataFrame")

        if self.period < 2:
            raise ValueError(f"Period must be at least 2, got {self.period}")

        # For grouped data, we'll validate length per group during decomposition
        if not self.group_columns and len(self.df) < 2 * self.period:
            raise ValueError(
                f"Time series length ({len(self.df)}) must be at least 2 * period ({2 * self.period})"
            )

    def _prepare_data(self) -> pd.Series:
        """Prepare the time series data for decomposition."""
        if self.timestamp_column:
            df_prepared = self.df.set_index(self.timestamp_column)
        else:
            df_prepared = self.df.copy()

        series = df_prepared[self.value_column]

        if series.isna().any():
            raise ValueError(
                f"Column '{self.value_column}' contains NaN values. "
                "Please handle missing values before decomposition."
            )

        return series

    def _decompose_single_group(self, group_df: PandasDataFrame) -> PandasDataFrame:
        """
        Decompose a single group (or the entire DataFrame if no grouping).

        Parameters
        ----------
        group_df : PandasDataFrame
            DataFrame for a single group

        Returns
        -------
        PandasDataFrame
            DataFrame with decomposition components added
        """
        # Validate group size
        if len(group_df) < 2 * self.period:
            raise ValueError(
                f"Group has {len(group_df)} observations, but needs at least "
                f"{2 * self.period} (2 * period) for decomposition"
            )

        # Prepare data
        if self.timestamp_column:
            series = group_df.set_index(self.timestamp_column)[self.value_column]
        else:
            series = group_df[self.value_column]

        if series.isna().any():
            raise ValueError(
                f"Column '{self.value_column}' contains NaN values. "
                "Please handle missing values before decomposition."
            )

        # Set default seasonal smoother length if not provided
        seasonal = self.seasonal
        if seasonal is None:
            seasonal = self.period + 1 if self.period % 2 == 0 else self.period

        # Create STL object and fit
        stl = STL(
            series,
            period=self.period,
            seasonal=seasonal,
            trend=self.trend,
            robust=self.robust,
        )
        result = stl.fit()

        # Add components to result
        result_df = group_df.copy()
        result_df["trend"] = result.trend.values
        result_df["seasonal"] = result.seasonal.values
        result_df["residual"] = result.resid.values

        return result_df

    def decompose(self) -> PandasDataFrame:
        """
        Perform STL decomposition.

        If group_columns is provided, decomposition is performed separately for each group.
        Each group must have at least 2 * period observations.

        Returns
        -------
        PandasDataFrame
            DataFrame containing the original data plus decomposed components:
            - trend: The trend component
            - seasonal: The seasonal component
            - residual: The residual component

        Raises
        ------
        ValueError
            If any group has insufficient data or contains NaN values
        """
        if self.group_columns:
            # Group by specified columns and decompose each group
            result_dfs = []

            for group_vals, group_df in self.df.groupby(self.group_columns):
                try:
                    decomposed_group = self._decompose_single_group(group_df)
                    result_dfs.append(decomposed_group)
                except ValueError as e:
                    group_str = dict(zip(self.group_columns, group_vals if isinstance(group_vals, tuple) else [group_vals]))
                    raise ValueError(f"Error in group {group_str}: {str(e)}")

            self.result_df = pd.concat(result_dfs, ignore_index=True)
        else:
            # No grouping - decompose entire DataFrame
            self.result_df = self._decompose_single_group(self.df)

        return self.result_df

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYTHON
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}
