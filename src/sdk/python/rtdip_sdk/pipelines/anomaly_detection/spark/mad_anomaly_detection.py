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
from pyspark.sql import DataFrame

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from ..interfaces import AnomalyDetectionInterface


class MadAnomalyDetection(AnomalyDetectionInterface):
    """
    Median Absolute Deviation (MAD) Anomaly Detection.
    """

    def __init__(self, threshold: float = 3.5):
        """
        Initialize the MAD-based anomaly detector.

        The threshold defines how many robust standard deviations (MAD z-score)
        a data point must deviate from the median to be classified as an anomaly.

        :param threshold:
            Robust z-score cutoff for anomaly detection.
            Values with ``abs(mad_zscore) > threshold`` are flagged as anomalies.
            Default is ``3.5``.
        :type threshold: float
        """
        self.threshold = threshold

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
        """
        Detect anomalies in a numeric time-series column using the Median Absolute
        Deviation (MAD) robust z-score method.

        Returns ONLY the rows classified as anomalies.
        See class documentation for detailed description.

        :param df:
            Input Spark DataFrame containing at least one numeric column named
            ``"value"``. This column is used for computing anomaly scores.
        :type df: DataFrame

        :return:
            A Spark DataFrame containing only the detected anomalies.
            Includes columns: ``value``, ``mad_zscore``, ``is_anomaly``.
        :rtype: DataFrame
        """

        # Spark → Pandas
        pdf = df.toPandas()

        median = pdf["value"].median()
        mad = np.median(np.abs(pdf["value"] - median))

        mad = max(mad, 1.0)  # clamp MAD to prevent over-sensitive detection

        if mad == 0:
            pdf["mad_zscore"] = 0
        else:
            pdf["mad_zscore"] = 0.6745 * (pdf["value"] - median) / mad

        pdf["is_anomaly"] = abs(pdf["mad_zscore"]) > self.threshold

        # keep only anomalies
        anomalies_pdf = pdf[pdf["is_anomaly"] == True].copy()

        # Pandas → Spark
        return df.sparkSession.createDataFrame(anomalies_pdf)
