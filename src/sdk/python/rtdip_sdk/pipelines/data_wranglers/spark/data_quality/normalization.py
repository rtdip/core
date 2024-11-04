# Copyright 2022 RTDIP
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
from enum import Enum

from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import functions as F
from typing import List
from ...interfaces import WranglerBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType

class NormalizationMethod(Enum):
    Z_SCORE = 1
    MIN_MAX = 2
    MEAN = 3


class Normalization(WranglerBaseInterface):
    """
    Applies normalization to multiple columns in a PySpark DataFrame using Z-Score, Min-Max or Mean normalization.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.monitoring.spark.data_quality.normalization import Normalization
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame

    normalization = Normalization(df, "z-score", ["value_column_1", "value_column_2"])
    normalized_df = normalization.normalize()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be normalized.
        method (str): Normalization method, either "z-score" or "min-max" or "mean".
        column_names (List[str]): List of columns in the DataFrame to be normalized.
    """

    df: PySparkDataFrame
    method: NormalizationMethod
    column_names: List[str]

    def __init__(self, df: PySparkDataFrame, method: NormalizationMethod, column_names: List[str]) -> None:

        # NOTE: Will throw TypeError before python 3.12, in 3.12 will return false on invalid values.
        if not method in NormalizationMethod:
            raise TypeError("Invalid normalization method")

        for column_name in column_names:
            if not column_name in df.columns:
                raise ValueError("{} not found in the DataFrame.".format(column_name))

        self.df = df
        self.method = method
        self.column_names = column_names

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

    def filter(self):
        pass

    def normalize(self) -> PySparkDataFrame:
        """
        Applies the specified normalization to each column in column_names.

        Returns:
            DataFrame: A PySpark DataFrame with the normalized values.
        """
        normalized_df = self.df
        if self.method == NormalizationMethod.Z_SCORE:
            for column in self.column_names:
                normalized_df = self._z_score_normalize(normalized_df, column)
        elif self.method == NormalizationMethod.MIN_MAX:
            for column in self.column_names:
                normalized_df = self._min_max_normalize(normalized_df, column)
        elif self.method == NormalizationMethod.MEAN:
            for column in self.column_names:
                normalized_df = self._mean_normalize(normalized_df, column)
        return normalized_df

    def denormalize(self, df) -> PySparkDataFrame:
        """
            Denormalizes the input DataFrame. Intended to be used by the denormalization component.

            Parameters:
                df (DataFrame): Dataframe containing the current data.
        """
        denormalized_df = self.df
        if self.method == NormalizationMethod.Z_SCORE:
            for column in self.column_names:
                denormalized_df = self._z_score_denormalize(denormalized_df, column)
        elif self.method == NormalizationMethod.MIN_MAX:
            for column in self.column_names:
                denormalized_df = self._min_max_denormalize(denormalized_df, column)
        elif self.method == NormalizationMethod.MEAN:
            for column in self.column_names:
                denormalized_df = self._mean_denormalize(denormalized_df, column)
        return denormalized_df

        return df

    def _z_score_normalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
        Private method to apply Z-Score normalization to the specified column.
        Z-Score normalization: (value - mean) / std_dev
        """
        mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
        std_dev_val = df.select(F.stddev(F.col(column))).collect()[0][0]

        return df.withColumn(
            f"{column}_zscore_normalized",
            (F.col(column) - F.lit(mean_val)) / F.lit(std_dev_val)
        )

    def _z_score_denormalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        # TODO
        return

    def _min_max_normalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
        Private method to apply Min-Max normalization to the specified column.
        Min-Max normalization: (value - min) / (max - min)
        """
        min_val = df.select(F.min(F.col(column))).collect()[0][0]
        max_val = df.select(F.max(F.col(column))).collect()[0][0]

        return df.withColumn(
            f"{column}_minmax_normalized",
            (F.col(column) - F.lit(min_val)) / (F.lit(max_val) - F.lit(min_val))
        )

    def _min_max_denormalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        # TODO
        return
    
    def _mean_normalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
        Private method to apply Mean normalization to the specified column.
        Mean normalization: (value - mean) / (max - min)
        """
        mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
        min_val = df.select(F.min(F.col(column))).collect()[0][0]
        max_val = df.select(F.max(F.col(column))).collect()[0][0]

        return df.withColumn(
            f"{column}_mean_normalized",
            (F.col(column) - F.lit(mean_val)) / (F.lit(max_val) - F.lit(min_val))
        )

    def _mean_denormalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        # TODO
        return
