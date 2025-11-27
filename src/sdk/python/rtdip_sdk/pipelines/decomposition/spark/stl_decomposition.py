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

    decomposer = STLDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        period=7
    )

    result = decomposer.decompose()
    ```

    Parameters:
        df (PySparkDataFrame): PySpark DataFrame containing the time series data.
        value_column (str): Name of the column containing the values to decompose.
        timestamp_column (str): Name of the column containing timestamps. If None, assumes ordered data.
        period (int): Periodicity of the seasonal component (e.g., 7 for weekly, 24 for hourly daily pattern).
        seasonal (int, optional): Length of the seasonal smoother. Must be odd.
        trend (int, optional): Length of the trend smoother. Must be odd.
        robust (bool): If True, use robust weights in the fitting procedure.
    """

    df: PySparkDataFrame
    value_column: str
    timestamp_column: str
    period: int
    seasonal: int
    trend: int
    robust: bool

    def __init__(
        self,
        df: PySparkDataFrame,
        value_column: str,
        timestamp_column: str = None,
        period: int = 7,
        seasonal: int = None,
        trend: int = None,
        robust: bool = False,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.period = period
        self.seasonal = seasonal
        self.trend = trend
        self.robust = robust

        # Validation
        if value_column not in df.columns:
            raise ValueError(f"Column '{value_column}' not found in DataFrame")
        if timestamp_column and timestamp_column not in df.columns:
            raise ValueError(f"Column '{timestamp_column}' not found in DataFrame")

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
        Performs STL decomposition on the time series.

        Returns:
            PySparkDataFrame: DataFrame with original columns plus 'trend', 'seasonal', and 'residual' columns.
        """
        from statsmodels.tsa.seasonal import STL

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

        if len(series) < 2 * self.period:
            raise ValueError(
                f"Time series length ({len(series)}) must be at least 2*period ({2*self.period})"
            )

        # Perform STL decomposition
        stl = STL(
            series,
            period=self.period,
            seasonal=self.seasonal if self.seasonal is not None else 7,
            trend=self.trend,
            robust=self.robust,
        )
        result = stl.fit()

        # Add decomposition results to dataframe
        pdf["trend"] = result.trend.values
        pdf["seasonal"] = result.seasonal.values
        pdf["residual"] = result.resid.values

        # Convert back to PySpark DataFrame
        result_df = self.df.sql_ctx.createDataFrame(pdf)

        return result_df
