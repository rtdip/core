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

from typing import Optional, Literal, List, Union
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from statsmodels.tsa.seasonal import seasonal_decompose

from ..interfaces import PandasDecompositionBaseInterface
from ..._pipeline_utils.models import Libraries, SystemType
from .period_utils import calculate_period_from_frequency


class ClassicalDecomposition(PandasDecompositionBaseInterface):
    """
    Decomposes a time series using classical decomposition with moving averages.

    Classical decomposition is a straightforward method that uses moving averages
    to extract the trend component. It supports both additive and multiplicative models.
    Use additive when seasonal variations are roughly constant, and multiplicative
    when seasonal variations change proportionally with the level of the series.

    Example
    -------
    ```python
    import pandas as pd
    import numpy as np
    from rtdip_sdk.pipelines.decomposition.pandas import ClassicalDecomposition

    # Example 1: Single time series - Additive decomposition
    dates = pd.date_range('2024-01-01', periods=365, freq='D')
    df = pd.DataFrame({
        'timestamp': dates,
        'value': np.sin(np.arange(365) * 2 * np.pi / 7) + np.arange(365) * 0.01 + np.random.randn(365) * 0.1
    })

    # Using explicit period
    decomposer = ClassicalDecomposition(
        df=df,
        value_column='value',
        timestamp_column='timestamp',
        model='additive',
        period=7  # Explicit: 7 days
    )
    result_df = decomposer.decompose()

    # Or using period string (auto-calculated from sampling frequency)
    decomposer = ClassicalDecomposition(
        df=df,
        value_column='value',
        timestamp_column='timestamp',
        model='additive',
        period='weekly'  # Automatically calculated
    )
    result_df = decomposer.decompose()

    # Example 2: Multiple time series (grouped by sensor)
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    df_multi = pd.DataFrame({
        'timestamp': dates.tolist() * 3,
        'sensor': ['A'] * 100 + ['B'] * 100 + ['C'] * 100,
        'value': np.random.randn(300)
    })

    decomposer_grouped = ClassicalDecomposition(
        df=df_multi,
        value_column='value',
        timestamp_column='timestamp',
        group_columns=['sensor'],
        model='additive',
        period=7
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
    model : {'additive', 'multiplicative'}
        Type of decomposition model:
        - 'additive': Y_t = T_t + S_t + R_t (for constant seasonal variations)
        - 'multiplicative': Y_t = T_t * S_t * R_t (for proportional seasonal variations)
    period : Union[int, str]
        Seasonal period. Can be:
        - Integer: Explicit period value (e.g., 7 for weekly, 24 for daily in hourly data)
        - String: Period name auto-calculated from sampling frequency
          Supported: 'minutely', 'hourly', 'daily', 'weekly', 'monthly',
          'quarterly', 'yearly'
        Examples:
        - 7 or 'weekly' for weekly patterns in daily data
        - 24 or 'daily' for daily patterns in hourly data
        - 720 or 'hourly' for hourly patterns in 5-second data
    two_sided : bool, default=True
        Whether to use centered moving averages
    extrapolate_trend : int or 'freq', default=0
        How many observations to extrapolate the trend at the boundaries

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
        model: Literal["additive", "multiplicative"] = "additive",
        period: Union[int, str] = 7,
        two_sided: bool = True,
        extrapolate_trend: int = 0,
    ):
        self.df = df.copy()
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.group_columns = group_columns
        self.model = model.lower()
        self.period_input = period  # Store original input
        self.period = None  # Will be resolved in _resolve_period
        self.two_sided = two_sided
        self.extrapolate_trend = extrapolate_trend
        self.result_df = None

        self._validate_inputs()

    def _validate_inputs(self):
        """Validate input parameters."""
        if self.value_column not in self.df.columns:
            raise ValueError(f"Column '{self.value_column}' not found in DataFrame")

        if self.timestamp_column and self.timestamp_column not in self.df.columns:
            raise ValueError(f"Column '{self.timestamp_column}' not found in DataFrame")

        if self.group_columns:
            missing_cols = [
                col for col in self.group_columns if col not in self.df.columns
            ]
            if missing_cols:
                raise ValueError(f"Group columns {missing_cols} not found in DataFrame")

        if self.model not in ["additive", "multiplicative"]:
            raise ValueError(
                f"Invalid model '{self.model}'. Must be 'additive' or 'multiplicative'"
            )

    def _resolve_period(self, group_df: PandasDataFrame) -> int:
        """
        Resolve period specification (string or integer) to integer value.

        Parameters
        ----------
        group_df : PandasDataFrame
            DataFrame for the group (needed to calculate period from frequency)

        Returns
        -------
        int
            Resolved period value
        """
        if isinstance(self.period_input, str):
            # String period name - calculate from sampling frequency
            if not self.timestamp_column:
                raise ValueError(
                    f"timestamp_column must be provided when using period strings like '{self.period_input}'"
                )

            period = calculate_period_from_frequency(
                df=group_df,
                timestamp_column=self.timestamp_column,
                period_name=self.period_input,
                min_cycles=2,
            )

            if period is None:
                raise ValueError(
                    f"Period '{self.period_input}' is not valid for this data. "
                    f"Either the calculated period is too small (<2) or there is insufficient "
                    f"data for at least 2 complete cycles."
                )

            return period
        elif isinstance(self.period_input, int):
            # Integer period - use directly
            if self.period_input < 2:
                raise ValueError(f"Period must be at least 2, got {self.period_input}")
            return self.period_input
        else:
            raise ValueError(
                f"Period must be int or str, got {type(self.period_input).__name__}"
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
        # Resolve period for this group
        resolved_period = self._resolve_period(group_df)

        # Validate group size
        if len(group_df) < 2 * resolved_period:
            raise ValueError(
                f"Group has {len(group_df)} observations, but needs at least "
                f"{2 * resolved_period} (2 * period) for decomposition"
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

        # Perform decomposition
        result = seasonal_decompose(
            series,
            model=self.model,
            period=resolved_period,
            two_sided=self.two_sided,
            extrapolate_trend=self.extrapolate_trend,
        )

        # Add components to result
        result_df = group_df.copy()
        result_df["trend"] = result.trend.values
        result_df["seasonal"] = result.seasonal.values
        result_df["residual"] = result.resid.values

        return result_df

    def decompose(self) -> PandasDataFrame:
        """
        Perform classical decomposition.

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
