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

    decomposer = ClassicalDecomposition(
        df=spark_df,
        value_column='value',
        timestamp_column='timestamp',
        model='additive',
        period=7
    )

    result = decomposer.decompose()
    ```

    Parameters:
        df (PySparkDataFrame): PySpark DataFrame containing the time series data.
        value_column (str): Name of the column containing the values to decompose.
        timestamp_column (str): Name of the column containing timestamps. If None, assumes ordered data.
        model (str): Type of seasonal component. Must be 'additive' or 'multiplicative'.
        period (int): Periodicity of the seasonal component.
        two_sided (bool): If True, use a centered moving average for trend estimation.
        extrapolate_trend (int or str): How many periods to extrapolate the trend at the boundaries.
    """

    df: PySparkDataFrame
    value_column: str
    timestamp_column: str
    model: str
    period: int
    two_sided: bool
    extrapolate_trend: int

    def __init__(
        self,
        df: PySparkDataFrame,
        value_column: str,
        timestamp_column: str = None,
        model: str = "additive",
        period: int = 7,
        two_sided: bool = True,
        extrapolate_trend: int = 0,
    ) -> None:
        self.df = df
        self.value_column = value_column
        self.timestamp_column = timestamp_column
        self.model = model
        self.period = period
        self.two_sided = two_sided
        self.extrapolate_trend = extrapolate_trend

        # Validation
        if value_column not in df.columns:
            raise ValueError(f"Column '{value_column}' not found in DataFrame")
        if timestamp_column and timestamp_column not in df.columns:
            raise ValueError(f"Column '{timestamp_column}' not found in DataFrame")
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

    def decompose(self) -> PySparkDataFrame:
        """
        Performs classical decomposition on the time series.

        Returns:
            PySparkDataFrame: DataFrame with original columns plus 'trend', 'seasonal', and 'residual' columns.
        """
        from statsmodels.tsa.seasonal import seasonal_decompose

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

        # Perform classical decomposition
        result = seasonal_decompose(
            series,
            model=self.model,
            period=self.period,
            two_sided=self.two_sided,
            extrapolate_trend=self.extrapolate_trend,
        )

        # Add decomposition results to dataframe
        pdf["trend"] = result.trend.values
        pdf["seasonal"] = result.seasonal.values
        pdf["residual"] = result.resid.values

        # Convert back to PySpark DataFrame
        result_df = self.df.sql_ctx.createDataFrame(pdf)

        return result_df
