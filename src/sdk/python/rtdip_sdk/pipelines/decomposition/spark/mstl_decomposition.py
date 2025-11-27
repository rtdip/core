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

    decomposer = MSTLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        periods=[24, 168]  # Daily and weekly patterns
    )

    result = decomposer.decompose()
    ```

    Parameters:
        df (PySparkDataFrame): PySpark DataFrame containing the time series data.
        value_column (str): Name of the column containing the values to decompose.
        timestamp_column (str): Name of the column containing timestamps. If None, assumes ordered data.
        periods (int or list): Periodicities of the seasonal components (e.g., [24, 168] for hourly data with daily and weekly patterns).
        windows (int or list, optional): Window sizes for seasonal smoothers. Must match length of periods if provided.
        iterate (int): Number of iterations for the MSTL algorithm.
        stl_kwargs (dict, optional): Additional keyword arguments to pass to the internal STL calls.
    """

    df: PySparkDataFrame
    value_column: str
    timestamp_column: str
    periods: list
    windows: list
    iterate: int
    stl_kwargs: dict

    def __init__(
        self,
        df: PySparkDataFrame,
        value_column: str,
        timestamp_column: str = None,
        periods: int = None,
        windows: int = None,
        iterate: int = 2,
        stl_kwargs: dict = None,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.timestamp_column = timestamp_column
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

    def decompose(self) -> PySparkDataFrame:
        """
        Performs MSTL decomposition on the time series.

        Returns:
            PySparkDataFrame: DataFrame with original columns plus 'trend', 'seasonal_X' (for each period X), and 'residual' columns.
        """
        from statsmodels.tsa.seasonal import MSTL

        # Convert to pandas
        pdf = self.df.toPandas()

        # Sort by timestamp if provided
        if self.timestamp_column:
            pdf = pdf.sort_values(self.timestamp_column)

        # Get the series
        series = pdf[self.value_column]

        # Validate data
        if series.isna().any():
            raise ValueError(
                f"Time series contains NaN values in column '{self.value_column}'"
            )

        max_period = max(self.periods)
        if len(series) < 2 * max_period:
            raise ValueError(
                f"Time series length ({len(series)}) must be at least 2*max_period ({2*max_period})"
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
        pdf["trend"] = result.trend.values

        # Handle seasonal components (can be Series or DataFrame)
        if len(self.periods) == 1:
            seasonal_col = f"seasonal_{self.periods[0]}"
            pdf[seasonal_col] = result.seasonal.values
        else:
            for i, period in enumerate(self.periods):
                seasonal_col = f"seasonal_{period}"
                pdf[seasonal_col] = result.seasonal[result.seasonal.columns[i]].values

        pdf["residual"] = result.resid.values

        # Convert back to PySpark DataFrame
        result_df = self.df.sql_ctx.createDataFrame(pdf)

        return result_df
