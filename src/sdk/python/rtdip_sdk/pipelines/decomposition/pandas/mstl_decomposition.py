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
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from statsmodels.tsa.seasonal import MSTL

from ..interfaces import PandasDecompositionBaseInterface
from ..._pipeline_utils.models import Libraries, SystemType


class MSTLDecomposition(PandasDecompositionBaseInterface):
    """
    Decomposes a time series with multiple seasonal patterns using MSTL.

    MSTL (Multiple Seasonal-Trend decomposition using Loess) extends STL to handle
    time series with multiple seasonal cycles. This is useful for high-frequency data
    with multiple seasonality patterns (e.g., hourly data with daily + weekly patterns,
    or daily data with weekly + yearly patterns).

    Example
    -------
    ```python
    import pandas as pd
    import numpy as np
    from rtdip_sdk.pipelines.decomposition.pandas import MSTLDecomposition

    # Create sample time series with multiple seasonalities
    # Hourly data with daily (24h) and weekly (168h) patterns
    n_hours = 24 * 30  # 30 days of hourly data
    dates = pd.date_range('2024-01-01', periods=n_hours, freq='H')

    daily_pattern = 5 * np.sin(2 * np.pi * np.arange(n_hours) / 24)
    weekly_pattern = 3 * np.sin(2 * np.pi * np.arange(n_hours) / 168)
    trend = np.linspace(10, 15, n_hours)
    noise = np.random.randn(n_hours) * 0.5

    df = pd.DataFrame({
        'timestamp': dates,
        'value': trend + daily_pattern + weekly_pattern + noise
    })

    # MSTL decomposition with multiple periods
    decomposer = MSTLDecomposition(
        df=df,
        value_column='value',
        timestamp_column='timestamp',
        periods=[24, 168],  # Daily and weekly seasonality
        windows=[25, 169]   # Seasonal smoother lengths (must be odd)
    )
    result_df = decomposer.decompose()

    # Result will have: trend, seasonal_24, seasonal_168, residual
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
    periods : Union[int, List[int]]
        Seasonal period(s). Can be a single integer or list of integers.
        Examples: 7 for weekly, [24, 168] for daily+weekly in hourly data,
        [7, 365] for weekly+yearly in daily data
    windows : Union[int, List[int]], optional
        Length(s) of seasonal smoother(s). Must be odd. If None, defaults based on periods.
        Should have same length as periods if provided as list.
    iterate : int, default=2
        Number of iterations for MSTL algorithm
    stl_kwargs : dict, optional
        Additional keyword arguments to pass to the underlying STL decomposition

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
        periods: Union[int, List[int]] = None,
        windows: Union[int, List[int]] = None,
        iterate: int = 2,
        stl_kwargs: Optional[dict] = None,
    ):
        self.df = df.copy()
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.periods = (
            periods if isinstance(periods, list) else [periods] if periods else [7]
        )
        self.windows = windows
        self.iterate = iterate
        self.stl_kwargs = stl_kwargs or {}
        self.result_df = None

        self._validate_inputs()

    def _validate_inputs(self):
        """Validate input parameters."""
        if self.value_column not in self.df.columns:
            raise ValueError(f"Column '{self.value_column}' not found in DataFrame")

        if self.timestamp_column and self.timestamp_column not in self.df.columns:
            raise ValueError(f"Column '{self.timestamp_column}' not found in DataFrame")

        if not self.periods:
            raise ValueError("At least one period must be specified")

        for period in self.periods:
            if period < 2:
                raise ValueError(f"All periods must be at least 2, got {period}")

        max_period = max(self.periods)
        if len(self.df) < 2 * max_period:
            raise ValueError(
                f"Time series length ({len(self.df)}) must be at least 2 * max_period ({2 * max_period})"
            )

        if self.windows is not None:
            windows_list = (
                self.windows if isinstance(self.windows, list) else [self.windows]
            )
            if len(windows_list) != len(self.periods):
                raise ValueError(
                    f"Length of windows ({len(windows_list)}) must match length of periods ({len(self.periods)})"
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

    def decompose(self) -> PandasDataFrame:
        """
        Perform MSTL decomposition.

        Returns
        -------
        PandasDataFrame
            DataFrame containing the original data plus decomposed components:
            - trend: The trend component
            - seasonal_{period}: Seasonal component for each period
            - residual: The residual component
        """
        series = self._prepare_data()

        # Create MSTL object and fit
        mstl = MSTL(
            series,
            periods=self.periods,
            windows=self.windows,
            iterate=self.iterate,
            stl_kwargs=self.stl_kwargs,
        )
        result = mstl.fit()

        # Create result DataFrame with original data
        self.result_df = self.df.copy()
        self.result_df["trend"] = result.trend.values

        # Add each seasonal component
        # Handle both Series (single period) and DataFrame (multiple periods)
        if len(self.periods) == 1:
            seasonal_col = f"seasonal_{self.periods[0]}"
            self.result_df[seasonal_col] = result.seasonal.values
        else:
            for i, period in enumerate(self.periods):
                seasonal_col = f"seasonal_{period}"
                self.result_df[seasonal_col] = result.seasonal[
                    result.seasonal.columns[i]
                ].values

        self.result_df["residual"] = result.resid.values

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
