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


class IqrAnomalyDetection(AnomalyDetectionInterface):
    """
    Interquartile Range (IQR) Anomaly Detection.
    """

    def __init__(self, threshold: float = 1.5):
        """
        Initialize the IQR-based anomaly detector.

        The threshold determines how many IQRs beyond Q1/Q3 a value must fall
        to be classified as an anomaly. Standard boxplot uses 1.5.

        :param threshold:
            IQR multiplier for anomaly bounds.
            Values outside [Q1 - threshold*IQR, Q3 + threshold*IQR] are flagged.
            Default is ``1.5`` (standard boxplot rule).
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
        Detect anomalies in a numeric time-series column using the Interquartile
        Range (IQR) method.

        Returns ONLY the rows classified as anomalies.

        :param df:
            Input Spark DataFrame containing at least one numeric column named
            ``"value"``. This column is used for computing anomaly bounds.
        :type df: DataFrame

        :return:
            A Spark DataFrame containing only the detected anomalies.
            Includes columns: ``value``, ``is_anomaly``.
        :rtype: DataFrame
        """

        # Spark → Pandas
        pdf = df.toPandas()

        # Calculate quartiles and IQR
        q1 = pdf["value"].quantile(0.25)
        q3 = pdf["value"].quantile(0.75)
        iqr = q3 - q1

        # Clamp IQR to prevent over-sensitive detection when data has no spread
        iqr = max(iqr, 1.0)

        # Define anomaly bounds
        lower_bound = q1 - self.threshold * iqr
        upper_bound = q3 + self.threshold * iqr

        # Flag values outside the bounds as anomalies
        pdf["is_anomaly"] = (pdf["value"] < lower_bound) | (pdf["value"] > upper_bound)

        # Keep only anomalies
        anomalies_pdf = pdf[pdf["is_anomaly"] == True].copy()

        # Pandas → Spark
        return df.sparkSession.createDataFrame(anomalies_pdf)


class IqrAnomalyDetectionRollingWindow(AnomalyDetectionInterface):
    """
    Interquartile Range (IQR) Anomaly Detection with Rolling Window.
    """

    def __init__(self, threshold: float = 1.5, window_size: int = 30):
        """
        Initialize the IQR-based anomaly detector with rolling window.

        The threshold determines how many IQRs beyond Q1/Q3 a value must fall
        to be classified as an anomaly. The rolling window adapts to trends.

        :param threshold:
            IQR multiplier for anomaly bounds.
            Values outside [Q1 - threshold*IQR, Q3 + threshold*IQR] are flagged.
            Default is ``1.5`` (standard boxplot rule).
        :type threshold: float

        :param window_size:
            Size of the rolling window (in number of data points) to compute
            Q1, Q3, and IQR for anomaly detection.
            Default is ``30``.
        :type window_size: int
        """
        self.threshold = threshold
        self.window_size = window_size

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
        Perform rolling IQR anomaly detection.

        Returns only the detected anomalies.

        :param df: Spark DataFrame containing a numeric "value" column.
        :return: Spark DataFrame containing only anomaly rows.
        """

        pdf = df.toPandas().sort_values("timestamp")

        # Rolling quartiles and IQR
        rolling_q1 = pdf["value"].rolling(self.window_size).quantile(0.25)
        rolling_q3 = pdf["value"].rolling(self.window_size).quantile(0.75)
        rolling_iqr = rolling_q3 - rolling_q1

        # Clamp IQR to prevent over-sensitivity
        rolling_iqr = rolling_iqr.apply(lambda x: max(x, 1.0))

        # Compute rolling bounds
        lower_bound = rolling_q1 - self.threshold * rolling_iqr
        upper_bound = rolling_q3 + self.threshold * rolling_iqr

        # Flag anomalies outside the rolling bounds
        pdf["is_anomaly"] = (pdf["value"] < lower_bound) | (pdf["value"] > upper_bound)

        # Keep only anomalies
        anomalies_pdf = pdf[pdf["is_anomaly"] == True].copy()

        return df.sparkSession.createDataFrame(anomalies_pdf)
