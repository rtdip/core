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

from typing import Optional, Literal
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from statsmodels.tsa.seasonal import seasonal_decompose

from ..interfaces import PandasDecompositionBaseInterface
from ..._pipeline_utils.models import Libraries, SystemType


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

    # Create sample time series data
    dates = pd.date_range('2024-01-01', periods=365, freq='D')
    df = pd.DataFrame({
        'timestamp': dates,
        'value': np.sin(np.arange(365) * 2 * np.pi / 7) + np.arange(365) * 0.01 + np.random.randn(365) * 0.1
    })

    # Additive decomposition
    decomposer = ClassicalDecomposition(
        df=df,
        value_column='value',
        timestamp_column='timestamp',
        model='additive',
        period=7
    )
    result_df = decomposer.decompose()

    # Multiplicative decomposition
    decomposer = ClassicalDecomposition(
        df=df,
        value_column='value',
        timestamp_column='timestamp',
        model='multiplicative',
        period=7
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
    model : {'additive', 'multiplicative'}
        Type of decomposition model:
        - 'additive': Y_t = T_t + S_t + R_t (for constant seasonal variations)
        - 'multiplicative': Y_t = T_t * S_t * R_t (for proportional seasonal variations)
    period : int
        Seasonal period (e.g., 7 for weekly, 24 for hourly daily patterns, 365 for yearly)
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
        model: Literal["additive", "multiplicative"] = "additive",
        period: int = 7,
        two_sided: bool = True,
        extrapolate_trend: int = 0,
    ):
        self.df = df.copy()
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.model = model.lower()
        self.period = period
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

        if self.model not in ["additive", "multiplicative"]:
            raise ValueError(
                f"Invalid model '{self.model}'. Must be 'additive' or 'multiplicative'"
            )

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
        Perform classical decomposition.

        Returns
        -------
        PandasDataFrame
            DataFrame containing the original data plus decomposed components:
            - trend: The trend component
            - seasonal: The seasonal component
            - residual: The residual component
        """
        series = self._prepare_data()

        # Perform decomposition
        result = seasonal_decompose(
            series,
            model=self.model,
            period=self.period,
            two_sided=self.two_sided,
            extrapolate_trend=self.extrapolate_trend,
        )

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
