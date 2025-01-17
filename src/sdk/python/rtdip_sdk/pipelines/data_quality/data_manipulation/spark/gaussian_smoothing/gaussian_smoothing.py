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
from scipy.ndimage import gaussian_filter1d
import pyspark.sql.functions as F
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql.types import DoubleType

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from ...interfaces import DataManipulationBaseInterface


class GaussianSmoothing(DataManipulationBaseInterface):
    """
    Applies Gaussian smoothing to time series data using scipy's gaussian_filter1d.
    """

    def __init__(
            self,
            df: PySparkDataFrame,
            sigma: float,
            id_col: str = "id",
            timestamp_col: str = "timestamp",
            value_col: str = "value",
    ) -> None:
        """
        Initialize the GaussianSmoothing transformer.

        Args:
            df: Input DataFrame
            sigma: Standard deviation for Gaussian kernel
            id_col: Name of ID column
            timestamp_col: Name of timestamp column
            value_col: Name of value column to smooth
        """
        # Validate input
        if not isinstance(df, PySparkDataFrame):
            raise TypeError("df must be a PySpark DataFrame")
        if not isinstance(sigma, (int, float)) or sigma <= 0:
            raise ValueError("sigma must be a positive number")

        # Validate column existence
        if id_col not in df.columns:
            raise ValueError(f"Column {id_col} not found in DataFrame")
        if timestamp_col not in df.columns:
            raise ValueError(f"Column {timestamp_col} not found in DataFrame")
        if value_col not in df.columns:
            raise ValueError(f"Column {value_col} not found in DataFrame")

        self.df = df
        self.sigma = sigma
        self.id_col = id_col
        self.timestamp_col = timestamp_col
        self.value_col = value_col

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
        libraries.add_pypi_dependency("scipy")
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def filter(self) -> PySparkDataFrame:
        """
               Apply Gaussian smoothing to the DataFrame using scipy's gaussian_filter1d.
               Updates the value column in-place with smoothed values.
               """
        # Convert to pandas for processing
        pdf = self.df.toPandas()

        # Group by ID and apply smoothing to each group
        for id_val in pdf[self.id_col].unique():
            mask = pdf[self.id_col] == id_val
            values = pdf.loc[mask, self.value_col].astype(float).values
            smoothed = gaussian_filter1d(values, sigma=self.sigma)
            pdf.loc[mask, self.value_col] = smoothed

        # Convert back to PySpark DataFrame
        spark = self.df.sparkSession
        result_df = spark.createDataFrame(pdf)

        return result_df