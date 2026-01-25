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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType
import math


class CyclicalEncoding(DataManipulationBaseInterface):
    """
    Applies cyclical encoding to a periodic column using sine/cosine transformation.

    Cyclical encoding captures the circular nature of periodic features where
    the end wraps around to the beginning (e.g., December is close to January,
    hour 23 is close to hour 0).

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.cyclical_encoding import CyclicalEncoding
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        (1, 100),
        (6, 200),
        (12, 300)
    ], ['month', 'value'])

    # Encode month cyclically (period=12 for months)
    encoder = CyclicalEncoding(df, column='month', period=12)
    result_df = encoder.filter_data()
    # Result will have columns: month, value, month_sin, month_cos
    ```

    Parameters:
        df (DataFrame): The PySpark DataFrame containing the column to encode.
        column (str): The name of the column to encode cyclically.
        period (int): The period of the cycle (e.g., 12 for months, 24 for hours, 7 for weekdays).
        drop_original (bool, optional): Whether to drop the original column. Defaults to False.
    """

    df: DataFrame
    column: str
    period: int
    drop_original: bool

    def __init__(
        self,
        df: DataFrame,
        column: str,
        period: int,
        drop_original: bool = False,
    ) -> None:
        self.df = df
        self.column = column
        self.period = period
        self.drop_original = drop_original

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

    def filter_data(self) -> DataFrame:
        """
        Applies cyclical encoding using sine and cosine transformations.

        Returns:
            DataFrame: DataFrame with added {column}_sin and {column}_cos columns.

        Raises:
            ValueError: If the DataFrame is None, column doesn't exist, or period <= 0.
        """
        if self.df is None:
            raise ValueError("The DataFrame is None.")

        if self.column not in self.df.columns:
            raise ValueError(f"Column '{self.column}' does not exist in the DataFrame.")

        if self.period <= 0:
            raise ValueError(f"Period must be positive, got {self.period}.")

        result_df = self.df

        # Apply sine/cosine transformation
        result_df = result_df.withColumn(
            f"{self.column}_sin",
            F.sin(2 * math.pi * F.col(self.column) / self.period),
        )
        result_df = result_df.withColumn(
            f"{self.column}_cos",
            F.cos(2 * math.pi * F.col(self.column) / self.period),
        )

        if self.drop_original:
            result_df = result_df.drop(self.column)

        return result_df
