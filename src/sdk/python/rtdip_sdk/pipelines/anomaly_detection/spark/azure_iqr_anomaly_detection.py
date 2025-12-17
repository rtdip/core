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
from pyspark.sql.functions import col

from rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from rtdip_sdk.pipelines.sources import PythonAzureBlobSource

from ..interfaces import AnomalyDetectionInterface
from .iqr_anomaly_detection import (
    IqrAnomalyDetection,
    IqrAnomalyDetectionRollingWindow,
)


class AzureIqrAnomalyDetection(AnomalyDetectionInterface):
    """
    Azure Blob IQR Anomaly Detection Pipeline.
    
    Combines Azure Blob Storage data ingestion with IQR-based anomaly detection.
    Provides a two-step workflow: read data from Azure, then detect anomalies.
    """

    def __init__(
        self,
        account_url: str,
        container_name: str,
        credential: str,
        file_pattern: str = "*.parquet",
        combine_blobs: bool = True,
        threshold: float = 1.5,
        use_rolling_window: bool = False,
        window_size: int = 30,
    ):
        """
        Initialize Azure IQR Anomaly Detection pipeline.

        :param account_url:
            Azure Storage account URL.
            Example: ``"https://accountname.blob.core.windows.net"``
        :type account_url: str

        :param container_name:
            Azure Blob container name.
            Example: ``"rtimedata"``
        :type container_name: str

        :param credential:
            Azure authentication credential (SAS token, access key, or connection string).
            Example: ``"?sv=2020-10-02&ss=btqf..."``
        :type credential: str

        :param file_pattern:
            Glob pattern for selecting files.
            Default is ``"*.parquet"``.
        :type file_pattern: str

        :param combine_blobs:
            If ``True``, combine multiple blob files into a single DataFrame.
            Default is ``True``.
        :type combine_blobs: bool

        :param threshold:
            IQR multiplier for anomaly bounds.
            Values outside [Q1 - threshold*IQR, Q3 + threshold*IQR] are flagged.
            Default is ``1.5`` (standard boxplot rule).
        :type threshold: float

        :param use_rolling_window:
            If ``True``, uses rolling window IQR detection.
            If ``False``, uses static IQR detection.
            Default is ``False``.
        :type use_rolling_window: bool

        :param window_size:
            Size of the rolling window (number of data points).
            Only used when ``use_rolling_window=True``.
            Default is ``30``.
        :type window_size: int
        """
        # Azure Blob configuration
        self.account_url = account_url
        self.container_name = container_name
        self.credential = credential
        self.file_pattern = file_pattern
        self.combine_blobs = combine_blobs

        # IQR detection parameters
        self.threshold = threshold
        self.use_rolling_window = use_rolling_window
        self.window_size = window_size

        # Initialize Azure Blob source
        self._source = PythonAzureBlobSource(
            account_url=self.account_url,
            container_name=self.container_name,
            credential=self.credential,
            file_pattern=self.file_pattern,
            combine_blobs=self.combine_blobs,
        )

        # Initialize IQR detector (will be used in detect method)
        if self.use_rolling_window:
            self._detector = IqrAnomalyDetectionRollingWindow(
                threshold=self.threshold, window_size=self.window_size
            )
        else:
            self._detector = IqrAnomalyDetection(threshold=self.threshold)

    @staticmethod
    def system_type() -> SystemType:
        """
        Returns the system type: PySpark.
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries() -> Libraries:
        """
        Returns required libraries.
        """
        return Libraries()

    @staticmethod
    def settings() -> dict:
        """
        Returns pipeline settings.
        """
        return {}

    def read_batch(self) -> DataFrame:
        """
        Read data from Azure Blob Storage.

        Returns a Spark DataFrame containing the ingested data from Azure Blob.
        Automatically handles schema detection and blob combination.

        :return:
            Spark DataFrame with data from Azure Blob Storage.
        :rtype: DataFrame

        Example
        -------
        >>> detector = AzureIqrAnomalyDetection(
        ...     account_url="https://account.blob.core.windows.net",
        ...     container_name="data",
        ...     credential="?sv=...",
        ...     threshold=1.5
        ... )
        >>> df = detector.read_batch()
        >>> df.show(5)
        """
        return self._source.read_batch()

    def detect(self, df: DataFrame) -> DataFrame:
        """
        Detect anomalies in the input DataFrame using IQR method.

        Uses either static or rolling window IQR detection based on
        the ``use_rolling_window`` parameter set during initialization.

        Returns ONLY the rows classified as anomalies.

        :param df:
            Input Spark DataFrame containing at least:
            - ``"value"`` column: Numeric values for anomaly detection
            - ``"timestamp"`` column: Timestamps (required for rolling window)
        :type df: DataFrame

        :return:
            A Spark DataFrame containing only the detected anomalies.
            Includes columns: ``value``, ``is_anomaly``, and others from input.
        :rtype: DataFrame

        Example
        -------
        >>> detector = AzureIqrAnomalyDetection(
        ...     account_url="https://account.blob.core.windows.net",
        ...     container_name="data",
        ...     credential="?sv=...",
        ...     threshold=1.5
        ... )
        >>> df = detector.read_batch()
        >>> anomalies = detector.detect(df)
        >>> print(f"Found {anomalies.count()} anomalies")
        """
        # Preprocess: Ensure required columns exist
        df = self._preprocess_dataframe(df)

        # Use the IQR detector to find anomalies
        return self._detector.detect(df)

    def _preprocess_dataframe(self, df: DataFrame) -> DataFrame:
        """
        Preprocess the DataFrame to ensure required columns exist.

        Handles common column naming variations from Shell data:
        - EventTime → timestamp
        - Value → value
        - Removes null values

        :param df: Input DataFrame
        :return: Preprocessed DataFrame
        """
        # Handle column name variations
        if "EventTime" in df.columns and "timestamp" not in df.columns:
            df = df.withColumnRenamed("EventTime", "timestamp")

        if "Value" in df.columns and "value" not in df.columns:
            df = df.withColumnRenamed("Value", "value")

        # Remove null values in critical columns
        if "value" in df.columns:
            df = df.filter(col("value").isNotNull())

        if self.use_rolling_window and "timestamp" in df.columns:
            df = df.filter(col("timestamp").isNotNull())

        return df

    def run_pipeline(
        self, tag_name: str = None, sample_fraction: float = None
    ) -> DataFrame:
        """
        Convenience method to run the full pipeline: read → preprocess → detect.

        This method combines ``read_batch()`` and ``detect()`` with optional
        filtering and sampling.

        :param tag_name:
            Optional tag name to filter data.
            If provided, filters DataFrame to this specific tag.
            Default is ``None`` (no filtering).
        :type tag_name: str

        :param sample_fraction:
            Optional fraction (0-1) to sample data.
            Useful for large datasets. Default is ``None`` (no sampling).
        :type sample_fraction: float

        :return:
            Spark DataFrame containing detected anomalies.
        :rtype: DataFrame

        Example
        -------
        >>> detector = AzureIqrAnomalyDetection(
        ...     account_url="https://account.blob.core.windows.net",
        ...     container_name="rtimedata",
        ...     credential="?sv=...",
        ...     threshold=1.5,
        ...     use_rolling_window=True
        ... )
        >>> # Run full pipeline on specific tag with 10% sample
        >>> anomalies = detector.run_pipeline(
        ...     tag_name="SENSOR_001",
        ...     sample_fraction=0.1
        ... )
        >>> anomalies.show()
        """
        # Step 1: Read from Azure
        df = self.read_batch()

        # Step 2: Optional filtering by tag
        if tag_name and "TagName" in df.columns:
            df = df.filter(col("TagName") == tag_name)

        # Step 3: Optional sampling
        if sample_fraction:
            df = df.sample(fraction=sample_fraction, seed=42)

        # Step 4: Detect anomalies
        return self.detect(df)
