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
import math

from .normalization import NormalizationBaseClass
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import functions as F


class NormalizationZScore(NormalizationBaseClass):
    """
    Implements Z-Score normalization for specified columns in a PySpark DataFrame.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.normalization.normalization_zscore import NormalizationZScore
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame

    normalization = NormalizationZScore(df, column_names=["value_column_1", "value_column_2"], in_place=False)
    normalized_df = normalization.filter()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be normalized.
        column_names (List[str]): List of columns in the DataFrame to be normalized.
        in_place (bool): If true, then result of normalization is stored in the same column.
    """

    NORMALIZED_COLUMN_NAME = "zscore"

    def _normalize_column(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
        Private method to apply Z-Score normalization to the specified column.
        Z-Score normalization: (value - mean) / std_dev
        """
        mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
        std_dev_val = df.select(F.stddev(F.col(column))).collect()[0][0]

        if math.isclose(std_dev_val, 0.0, abs_tol=10e-8) or not math.isfinite(
            std_dev_val
        ):
            raise ZeroDivisionError("Division by Zero in ZScore")

        store_column = self._get_norm_column_name(column)
        self.reversal_value = [mean_val, std_dev_val]

        return df.withColumn(
            store_column, (F.col(column) - F.lit(mean_val)) / F.lit(std_dev_val)
        )

    def _denormalize_column(
        self, df: PySparkDataFrame, column: str
    ) -> PySparkDataFrame:
        """
        Private method to revert Z-Score normalization to the specified column.
        Z-Score denormalization: normalized_value * std_dev + mean = value
        """
        mean_val = self.reversal_value[0]
        std_dev_val = self.reversal_value[1]

        store_column = self._get_norm_column_name(column)

        return df.withColumn(
            store_column, F.col(column) * F.lit(std_dev_val) + F.lit(mean_val)
        )
