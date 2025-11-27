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

from typing import Optional
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

    # Create sample time series data
    dates = pd.date_range('2024-01-01', periods=365, freq='D')
    df = pd.DataFrame({
        'timestamp': dates,
        'value': np.sin(np.arange(365) * 2 * np.pi / 7) + np.arange(365) * 0.01 + np.random.randn(365) * 0.1
    })

    # STL decomposition
    decomposer = STLDecomposition(
        df=df,
        value_column='value',
        timestamp_column='timestamp',
        period=7,
        robust=True
    )
    result_df = decomposer.decompose()
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
        period: int = 7,
        seasonal: Optional[int] = None,
        trend: Optional[int] = None,
        robust: bool = False,
    ):
        self.df = df.copy()
        self.value_column = value_column
        self.timestamp_column = timestamp_column
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

        if self.period < 2:
            raise ValueError(f"Period must be at least 2, got {self.period}")

        if len(self.df) < 2 * self.period:
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

    def decompose(self) -> PandasDataFrame:
        """
        Perform STL decomposition.

        Returns
        -------
        PandasDataFrame
            DataFrame containing the original data plus decomposed components:
            - trend: The trend component
            - seasonal: The seasonal component
            - residual: The residual component
        """
        series = self._prepare_data()

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

        # Create result DataFrame with original data
        self.result_df = self.df.copy()
        self.result_df["trend"] = result.trend.values
        self.result_df["seasonal"] = result.seasonal.values
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
