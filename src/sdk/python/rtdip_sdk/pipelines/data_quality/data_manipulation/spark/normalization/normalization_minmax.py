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


class NormalizationMinMax(NormalizationBaseClass):
    """
    Implements Min-Max normalization for specified columns in a PySpark DataFrame.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.normalization.normalization_minmax import NormalizationMinMax
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame

    normalization = NormalizationMinMax(df, column_names=["value_column_1", "value_column_2"], in_place=False)
    normalized_df = normalization.filter_data()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be normalized.
        column_names (List[str]): List of columns in the DataFrame to be normalized.
        in_place (bool): If true, then result of normalization is stored in the same column.
    """

    NORMALIZED_COLUMN_NAME = "minmax"

    def _normalize_column(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
        Private method to revert Min-Max normalization to the specified column.
        Min-Max denormalization: normalized_value * (max - min) + min = value
        """
        min_val = df.select(F.min(F.col(column))).collect()[0][0]
        max_val = df.select(F.max(F.col(column))).collect()[0][0]

        divisor = max_val - min_val
        if math.isclose(divisor, 0.0, abs_tol=10e-8) or not math.isfinite(divisor):
            raise ZeroDivisionError("Division by Zero in MinMax")

        store_column = self._get_norm_column_name(column)
        self.reversal_value = [min_val, max_val]

        return df.withColumn(
            store_column,
            (F.col(column) - F.lit(min_val)) / (F.lit(max_val) - F.lit(min_val)),
        )

    def _denormalize_column(
        self, df: PySparkDataFrame, column: str
    ) -> PySparkDataFrame:
        """
        Private method to revert Z-Score normalization to the specified column.
        Z-Score denormalization: normalized_value * std_dev + mean = value
        """
        min_val = self.reversal_value[0]
        max_val = self.reversal_value[1]

        store_column = self._get_norm_column_name(column)

        return df.withColumn(
            store_column,
            (F.col(column) * (F.lit(max_val) - F.lit(min_val))) + F.lit(min_val),
        )
