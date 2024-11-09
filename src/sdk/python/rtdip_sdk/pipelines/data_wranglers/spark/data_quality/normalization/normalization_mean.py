# Copyright 2024 RTDIP
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


class NormalizationMean(NormalizationBaseClass):

    NORMALIZED_COLUMN_NAME = "mean"

    def _normalize_column(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        """
        Private method to apply Mean normalization to the specified column.
        Mean normalization: (value - mean) / (max - min)
        """
        mean_val = df.select(F.mean(F.col(column))).collect()[0][0]
        min_val = df.select(F.min(F.col(column))).collect()[0][0]
        max_val = df.select(F.max(F.col(column))).collect()[0][0]

        divisor = max_val - min_val
        if math.isclose(divisor, 0.0, abs_tol=10e-8) or not math.isfinite(divisor):
            raise ZeroDivisionError("Division by Zero in Mean")

        store_column = self._get_norm_column_name(column)
        self.reversal_value = [mean_val, min_val, max_val]

        return df.withColumn(
            store_column,
            (F.col(column) - F.lit(mean_val)) / (F.lit(max_val) - F.lit(min_val)),
        )

    def _denormalize_column(
        self, df: PySparkDataFrame, column: str
    ) -> PySparkDataFrame:
        """
        Private method to revert Mean normalization to the specified column.
        Mean denormalization: normalized_value * (max - min) + mean = value
        """
        mean_val = self.reversal_value[0]
        min_val = self.reversal_value[1]
        max_val = self.reversal_value[2]

        store_column = self._get_norm_column_name(column)

        return df.withColumn(
            store_column,
            F.col(column) * (F.lit(max_val) - F.lit(min_val)) + F.lit(mean_val),
        )
