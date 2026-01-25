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
import pandas as pd

from pyspark.sql import DataFrame
from typing import Optional, List, Union

from ...._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from ...interfaces import AnomalyDetectionInterface
from ....decomposition.spark.stl_decomposition import STLDecomposition
from ....decomposition.spark.mstl_decomposition import MSTLDecomposition

from .interfaces import MadScorer


class GlobalMadScorer(MadScorer):
    def score(self, series: pd.Series) -> pd.Series:
        median = series.median()
        mad = np.median(np.abs(series - median))
        mad = max(mad, 1.0)

        return 0.6745 * (series - median) / mad


class RollingMadScorer(MadScorer):
    def __init__(self, threshold: float = 3.5, window_size: int = 30):
        super().__init__(threshold)
        self.window_size = window_size

    def score(self, series: pd.Series) -> pd.Series:
        rolling_median = series.rolling(self.window_size).median()
        rolling_mad = (
            series.rolling(self.window_size)
            .apply(lambda x: np.median(np.abs(x - np.median(x))), raw=True)
            .clip(lower=1.0)
        )

        return 0.6745 * (series - rolling_median) / rolling_mad


class MadAnomalyDetection(AnomalyDetectionInterface):
    """
    Median Absolute Deviation (MAD) Anomaly Detection.
    """

    def __init__(self, scorer: Optional[MadScorer] = None):
        self.scorer = scorer or GlobalMadScorer()

    @staticmethod
    def system_type() -> SystemType:
        return SystemType.PYSPARK

    @staticmethod
    def libraries() -> Libraries:
        return Libraries()

    @staticmethod
    def settings() -> dict:
        return {}

    def detect(self, df: DataFrame) -> DataFrame:
        pdf = df.toPandas()

        scores = self.scorer.score(pdf["value"])
        pdf["mad_zscore"] = scores
        pdf["is_anomaly"] = self.scorer.is_anomaly(scores)

        return df.sparkSession.createDataFrame(pdf[pdf["is_anomaly"]].copy())


class DecompositionMadAnomalyDetection(AnomalyDetectionInterface):
    """
    STL + MAD anomaly detection.

    1) Apply STL decomposition to remove trend & seasonality
    2) Apply MAD on the residual column
    3) Return ONLY rows flagged as anomalies
    """

    def __init__(
        self,
        scorer: MadScorer,
        decomposition: str = "mstl",
        period: Union[int, str] = 24,
        group_columns: Optional[List[str]] = None,
        timestamp_column: str = "timestamp",
        value_column: str = "value",
    ):
        self.scorer = scorer
        self.decomposition = decomposition
        self.period = period
        self.group_columns = group_columns
        self.timestamp_column = timestamp_column
        self.value_column = value_column

    @staticmethod
    def system_type() -> SystemType:
        return SystemType.PYSPARK

    @staticmethod
    def libraries() -> Libraries:
        return Libraries()

    @staticmethod
    def settings() -> dict:
        return {}

    def _decompose(self, df: DataFrame) -> DataFrame:
        """
        Custom decomposition logic.

        :param df: Input DataFrame
        :type df: DataFrame
        :return: Decomposed DataFrame
        :rtype: DataFrame
        """
        if self.decomposition == "stl":

            return STLDecomposition(
                df=df,
                value_column=self.value_column,
                timestamp_column=self.timestamp_column,
                group_columns=self.group_columns,
                period=self.period,
            ).decompose()

        elif self.decomposition == "mstl":

            return MSTLDecomposition(
                df=df,
                value_column=self.value_column,
                timestamp_column=self.timestamp_column,
                group_columns=self.group_columns,
                periods=self.period,
            ).decompose()
        else:
            raise ValueError(f"Unsupported decomposition method: {self.decomposition}")

    def detect(self, df: DataFrame) -> DataFrame:
        decomposed_df = self._decompose(df)
        pdf = decomposed_df.toPandas().sort_values(self.timestamp_column)

        scores = self.scorer.score(pdf["residual"])
        pdf["mad_zscore"] = scores
        pdf["is_anomaly"] = self.scorer.is_anomaly(scores)

        return df.sparkSession.createDataFrame(pdf[pdf["is_anomaly"]].copy())
