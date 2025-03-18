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
import numpy as np
from pyspark.sql.types import FloatType
from scipy.ndimage import gaussian_filter1d
from pyspark.sql import DataFrame as PySparkDataFrame, Window
from pyspark.sql import functions as F

from ...._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from ..interfaces import DataManipulationBaseInterface


class GaussianSmoothing(DataManipulationBaseInterface):
    """
    Applies Gaussian smoothing to a PySpark DataFrame. This method smooths the values in a specified column
    using a Gaussian filter, which helps reduce noise and fluctuations in time-series or spatial data.

    The smoothing can be performed in two modes:
    - **Temporal mode**: Applies smoothing along the time axis within each unique ID.
    - **Spatial mode**: Applies smoothing across different IDs for the same timestamp.

    Example
    --------
    ```python
    from pyspark.sql import SparkSession
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.gaussian_smoothing import GaussianSmoothing


    spark = SparkSession.builder.getOrCreate()
    df = ...  # Load your PySpark DataFrame

    smoothed_df = GaussianSmoothing(
        df=df,
        sigma=2.0,
        mode="temporal",
        id_col="sensor_id",
        timestamp_col="timestamp",
        value_col="measurement"
    ).filter_data()

    smoothed_df.show()
    ```

    Parameters:
        df (PySparkDataFrame): The input PySpark DataFrame.
        sigma (float): The standard deviation for the Gaussian kernel, controlling the amount of smoothing.
        mode (str, optional): The smoothing mode, either `"temporal"` (default) or `"spatial"`.
        id_col (str, optional): The name of the column representing unique entity IDs (default: `"id"`).
        timestamp_col (str, optional): The name of the column representing timestamps (default: `"timestamp"`).
        value_col (str, optional): The name of the column containing the values to be smoothed (default: `"value"`).

    Raises:
        TypeError: If `df` is not a PySpark DataFrame.
        ValueError: If `sigma` is not a positive number.
        ValueError: If `mode` is not `"temporal"` or `"spatial"`.
        ValueError: If `id_col`, `timestamp_col`, or `value_col` are not found in the DataFrame.
    """

    def __init__(
        self,
        df: PySparkDataFrame,
        sigma: float,
        mode: str = "temporal",
        id_col: str = "id",
        timestamp_col: str = "timestamp",
        value_col: str = "value",
    ) -> None:
        if not isinstance(df, PySparkDataFrame):
            raise TypeError("df must be a PySpark DataFrame")
        if not isinstance(sigma, (int, float)) or sigma <= 0:
            raise ValueError("sigma must be a positive number")
        if mode not in ["temporal", "spatial"]:
            raise ValueError("mode must be either 'temporal' or 'spatial'")

        if id_col not in df.columns:
            raise ValueError(f"Column {id_col} not found in DataFrame")
        if timestamp_col not in df.columns:
            raise ValueError(f"Column {timestamp_col} not found in DataFrame")
        if value_col not in df.columns:
            raise ValueError(f"Column {value_col} not found in DataFrame")

        self.df = df
        self.sigma = sigma
        self.mode = mode
        self.id_col = id_col
        self.timestamp_col = timestamp_col
        self.value_col = value_col

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    @staticmethod
    def create_gaussian_smoother(sigma_value):
        def apply_gaussian(values):
            if not values:
                return None
            values_array = np.array([float(v) for v in values])
            smoothed = gaussian_filter1d(values_array, sigma=sigma_value)
            return float(smoothed[-1])

        return apply_gaussian

    def filter_data(self) -> PySparkDataFrame:

        smooth_udf = F.udf(self.create_gaussian_smoother(self.sigma), FloatType())

        if self.mode == "temporal":
            window = (
                Window.partitionBy(self.id_col)
                .orderBy(self.timestamp_col)
                .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )
        else:  # spatial mode
            window = (
                Window.partitionBy(self.timestamp_col)
                .orderBy(self.id_col)
                .rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            )

        collect_list_expr = F.collect_list(F.col(self.value_col)).over(window)

        return self.df.withColumn(self.value_col, smooth_udf(collect_list_expr))
