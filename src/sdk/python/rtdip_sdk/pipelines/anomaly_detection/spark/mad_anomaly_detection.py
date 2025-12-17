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
from typing import Optional, List

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from ..interfaces import AnomalyDetectionInterface
from ...decomposition.spark.stl_decomposition import STLDecomposition


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

        # clamp MAD to prevent over-sensitive detection
        mad = float(max(mad, 1.0))

        if mad == 0:
            pdf["mad_zscore"] = 0
        else:
            pdf["mad_zscore"] = 0.6745 * (pdf["value"] - median) / mad

        pdf["is_anomaly"] = abs(pdf["mad_zscore"]) > self.threshold

        # keep only anomalies
        anomalies_pdf = pdf[pdf["is_anomaly"] == True].copy()

        # Pandas → Spark
        return df.sparkSession.createDataFrame(anomalies_pdf)


class MadAnomalyDetectionRollingWindow(AnomalyDetectionInterface):
    """
    Median Absolute Deviation (MAD) Anomaly Detection with Rolling Window.
    """

    def __init__(self, threshold: float = 3.5, window_size: int = 30):
        """
        Initialize the MAD-based anomaly detector with rolling window.

        The threshold defines how many robust standard deviations (MAD z-score)
        a data point must deviate from the median to be classified as an anomaly.

        :param threshold:
            Robust z-score cutoff for anomaly detection.
            Values with ``abs(mad_zscore) > threshold`` are flagged as anomalies.
            Default is ``3.5``.
        :type threshold: float

        :param window_size:
            Size of the rolling window (in number of data points) to compute
            median and MAD for anomaly detection.
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
        Perform rolling MAD anomaly detection.

        Returns only the detected anomalies.

        :param df: Spark DataFrame containing a numeric "value" column.
        :return: Spark DataFrame containing only anomaly rows.
        """

        pdf = df.toPandas().sort_values("timestamp")

        # Rolling median & MAD
        rolling_median = pdf["value"].rolling(self.window_size).median()
        rolling_mad = (
            pdf["value"]
            .rolling(self.window_size)
            .apply(lambda x: np.median(np.abs(x - np.median(x))), raw=True)
        )

        rolling_mad = rolling_mad.apply(lambda x: max(x, 1.0))

        # Robust rolling z-score
        pdf["rolling_mad_z"] = 0.6745 * (pdf["value"] - rolling_median) / rolling_mad
        pdf["is_anomaly"] = pdf["rolling_mad_z"].abs() > self.threshold

        # keep only anomalies
        anomalies_pdf = pdf[pdf["is_anomaly"] == True].copy()

        return df.sparkSession.createDataFrame(anomalies_pdf)


class StlMadAnomalyDetection(AnomalyDetectionInterface):
    """
    STL + MAD anomaly detection.

    1) Apply STL decomposition to remove trend & seasonality
    2) Apply MAD on the residual column
    3) Return ONLY rows flagged as anomalies
    """

    def __init__(
        self,
        period: int = 24,
        threshold: float = 3.5,
        group_columns: Optional[List[str]] = None,
        timestamp_column: str = "timestamp",
        value_column: str = "value",
    ):
        """
        :param period:
            STL seasonal period (e.g., 24 for hourly daily seasonality)
        :param threshold:
            MAD cutoff for marking anomalies
        :param group_columns:
            Optional group-by columns for STL decomposition
        :param timestamp_column:
            Timestamp column name
        :param value_column:
            Value column name
        """

        self.period = period
        self.threshold = threshold
        self.group_columns = group_columns
        self.timestamp_column = timestamp_column
        self.value_column = value_column

    # RTDIP required static methods
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
        Performs STL decomposition and then MAD-based anomaly detection
        on the STL residual component.

        Returns a Spark DataFrame containing ONLY the anomalies.
        """

        # Step 1: STL decomposition
        stl = STLDecomposition(
            df=df,
            value_column=self.value_column,
            timestamp_column=self.timestamp_column,
            group_columns=self.group_columns,
            period=self.period,
        )

        decomposed_df = stl.decompose()

        # Spark -> Pandas
        pdf = decomposed_df.toPandas()
        pdf = pdf.sort_values(self.timestamp_column)

        residuals = pdf["residual"].to_numpy(dtype=float)

        median = np.median(residuals)
        mad = np.median(np.abs(residuals - median))

        mad = float(max(mad, 1.0))

        pdf["mad_zscore"] = 0.6745 * (residuals - median) / mad
        pdf["is_anomaly"] = pdf["mad_zscore"].abs() > self.threshold

        # keep ONLY anomalies
        anomalies_pdf = pdf[pdf["is_anomaly"] == True].copy()

        return df.sparkSession.createDataFrame(anomalies_pdf)
