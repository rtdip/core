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
    Z_SCORE = 'zscore'
    MIN_MAX = 'minmax'
    MEAN = 'mean'


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
        in_place (bool): If true, then result of normalization is stored in the same column.
    """

    df: PySparkDataFrame
    method: NormalizationMethod
    column_names: List[str]
    in_place: bool

    reversal_value: List[float]

    # Appended to column name if new column is added
    NORMALIZATION_NAME_POSTFIX: str = "normalization"

    def __init__(self, df: PySparkDataFrame, method: NormalizationMethod, column_names: List[str],
                 in_place: bool = False) -> None:

        # NOTE: Will throw TypeError before python 3.12, in 3.12 will return false on invalid values.
        if not method in NormalizationMethod:
            raise TypeError("Invalid normalization method")

        for column_name in column_names:
            if not column_name in df.columns:
                raise ValueError("{} not found in the DataFrame.".format(column_name))

        self.df = df
        self.method = method
        self.column_names = column_names
        self.in_place = in_place

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
        if not self.in_place:
            for column in self.column_names:
                denormalized_df = denormalized_df.drop(self._get_norm_column_name(column))
            return denormalized_df

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

    def _z_score_normalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
        Private method to apply Z-Score normalization to the specified column.
        Z-Score normalization: (value - mean) / std_dev
        """
        mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
        std_dev_val = df.select(F.stddev(F.col(column))).collect()[0][0]

        store_column = self._get_norm_column_name(column)
        self.reversal_value = [mean_val, std_dev_val]

        return df.withColumn(
            store_column,
            (F.col(column) - F.lit(mean_val)) / F.lit(std_dev_val)
        )

    def _z_score_denormalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
            Private method to revert Z-Score normalization to the specified column.
            Z-Score denormalization: normalized_value * std_dev + mean = value
        """
        mean_val = self.reversal_value[0]
        std_dev_val = self.reversal_value[1]

        store_column = self._get_norm_column_name(column)

        return df.withColumn(
            store_column,
            F.col(column) * F.lit(std_dev_val) + F.lit(mean_val)
        )

    def _min_max_normalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
            Private method to apply Min-Max normalization to the specified column.
            Min-Max normalization: (value - min) / (max - min)
        """
        min_val = df.select(F.min(F.col(column))).collect()[0][0]
        max_val = df.select(F.max(F.col(column))).collect()[0][0]

        store_column = self._get_norm_column_name(column)
        self.reversal_value = [min_val, max_val]

        return df.withColumn(
            store_column,
            (F.col(column) - F.lit(min_val)) / (F.lit(max_val) - F.lit(min_val))
        )

    def _min_max_denormalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
            Private method to revert Min-Max normalization to the specified column.
            Min-Max denormalization: normalized_value * (max - min) + min = value
        """
        min_val = self.reversal_value[0]
        max_val = self.reversal_value[1]

        store_column = self._get_norm_column_name(column)

        return df.withColumn(
            store_column,
            F.col(column) * (F.lit(max_val) - F.lit(min_val)) + F.lit(min_val)
        )
    
    def _mean_normalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
        Private method to apply Mean normalization to the specified column.
        Mean normalization: (value - mean) / (max - min)
        """
        mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
        min_val = df.select(F.min(F.col(column))).collect()[0][0]
        max_val = df.select(F.max(F.col(column))).collect()[0][0]

        store_column = self._get_norm_column_name(column)
        self.reversal_value = [mean_val, min_val, max_val]

        return df.withColumn(
            store_column,
            (F.col(column) - F.lit(mean_val)) / (F.lit(max_val) - F.lit(min_val))
        )

    def _mean_denormalize(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
            Private method to revert Mean normalization to the specified column.
            Mean denormalization: normalized_value * (max - min) + mean = value
        """
        mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
        min_val = df.select(F.min(F.col(column))).collect()[0][0]
        max_val = df.select(F.max(F.col(column))).collect()[0][0]

        store_column = self._get_norm_column_name(column)
        self.reversal_value = [mean_val, min_val, max_val]

        return df.withColumn(
            store_column,
            F.col(column) * (F.lit(max_val) - F.lit(min_val)) + F.lit(mean_val)
        )

    def _get_norm_column_name(self, column_name: str) -> str:
        if not self.in_place:
            return f"{column_name}_{self.method.value}_{self.NORMALIZATION_NAME_POSTFIX}"
        else:
            return column_name