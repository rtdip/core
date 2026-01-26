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
    """
    Computes anomaly scores using the global Median Absolute Deviation (MAD) method.

    This scorer applies the robust MAD-based z-score normalization to an entire
    time series using a single global median and MAD value. It is resistant to
    outliers and suitable for detecting global anomalies in stationary or
    weakly non-stationary signals.

    The anomaly score is computed as:

        score = 0.6745 * (x - median) / MAD

    where the constant 0.6745 ensures consistency with the standard deviation
    for normally distributed data.

    A minimum MAD value of 1.0 is enforced to avoid division by zero and numerical
    instability.

    This component operates on Pandas Series objects.

    Example
    -------
    ```python
    import pandas as pd
    from rtdip_sdk.pipelines.anomaly_detection.mad import GlobalMadScorer

    data = pd.Series([10, 11, 10, 12, 500, 11, 10])

    scorer = GlobalMadScorer()
    scores = scorer.score(data)

    print(scores)
    ```
    """

    def score(self, series: pd.Series) -> pd.Series:
        """
        Computes MAD-based anomaly scores for a Pandas Series.

        Parameters:
            series (pd.Series): Input time series containing numeric values to be scored.

        Returns:
            pd.Series: MAD-based anomaly scores for each observation in the input series.
        """
        median = series.median()
        mad = np.median(np.abs(series - median))
        mad = max(mad, 1.0)

        return 0.6745 * (series - median) / mad


class RollingMadScorer(MadScorer):
    """
    Computes anomaly scores using a rolling window Median Absolute Deviation (MAD) method.

    This scorer applies MAD-based z-score normalization over a sliding window to
    capture local variations in the time series. Unlike the global MAD approach,
    this method adapts to non-stationary signals by recomputing the median and MAD
    for each window position.

    The anomaly score is computed as:

        score = 0.6745 * (x - rolling_median) / rolling_MAD

    where the constant 0.6745 ensures consistency with the standard deviation
    for normally distributed data.

    A minimum MAD value of 1.0 is enforced to avoid division by zero and numerical
    instability.

    This component operates on Pandas Series objects.

    Example
    -------
    ```python
    import pandas as pd
    from rtdip_sdk.pipelines.anomaly_detection.mad import RollingMadScorer

    data = pd.Series([10, 11, 10, 12, 500, 11, 10, 9, 10, 12])

    scorer = RollingMadScorer(window_size=5)
    scores = scorer.score(data)

    print(scores)
    ```

    Parameters:
        threshold (float): Threshold applied to anomaly scores to flag anomalies.
            Defaults to 3.5.
        window_size (int): Size of the rolling window used to compute local median
            and MAD values. Defaults to 30.
    """

    def __init__(self, threshold: float = 3.5, window_size: int = 30):
        super().__init__(threshold)
        self.window_size = window_size

    def score(self, series: pd.Series) -> pd.Series:
        """
        Computes rolling MAD-based anomaly scores for a Pandas Series.

        Parameters:
            series (pd.Series): Input time series containing numeric values to be scored.

        Returns:
            pd.Series: Rolling MAD-based anomaly scores for each observation in the input series.
        """
        rolling_median = series.rolling(self.window_size).median()
        rolling_mad = (
            series.rolling(self.window_size)
            .apply(lambda x: np.median(np.abs(x - np.median(x))), raw=True)
            .clip(lower=1.0)
        )

        return 0.6745 * (series - rolling_median) / rolling_mad


class MadAnomalyDetection(AnomalyDetectionInterface):
    """
    Detects anomalies in time series data using the Median Absolute Deviation (MAD) method.

    This anomaly detection component applies a MAD-based scoring strategy to identify
    outliers in a time series. It converts the input PySpark DataFrame into a Pandas
    DataFrame for local computation, applies the configured MAD scorer, and returns
    only the rows classified as anomalies.

    By default, the `GlobalMadScorer` is used, which computes anomaly scores based on
    global median and MAD statistics. Alternative scorers such as `RollingMadScorer`
    can be injected to support adaptive, window-based anomaly detection.

    This component is intended for batch-oriented anomaly detection pipelines using
    PySpark as the execution backend.

    Example
    -------
    ```python
    from pyspark.sql import SparkSession
    from rtdip_sdk.pipelines.anomaly_detection.mad import MadAnomalyDetection, RollingMadScorer

    spark = SparkSession.builder.getOrCreate()

    spark_df = spark.createDataFrame(
        [
            ("2024-01-01", 10),
            ("2024-01-02", 11),
            ("2024-01-03", 500),
            ("2024-01-04", 12),
        ],
        ["timestamp", "value"]
    )

    detector = MadAnomalyDetection(
        scorer=RollingMadScorer(window_size=3)
    )

    anomalies_df = detector.detect(spark_df)
    anomalies_df.show()
    ```

    Parameters:
        scorer (Optional[MadScorer]): MAD-based scoring strategy used to compute anomaly
            scores. If None, `GlobalMadScorer` is used by default.
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
        """
        Detects anomalies in the input DataFrame using the configured MAD scorer.

        The method computes MAD-based anomaly scores on the `value` column, adds the
        columns `mad_zscore` and `is_anomaly`, and returns only the rows classified
        as anomalies.

        Parameters:
            df (DataFrame): Input PySpark DataFrame containing at least a `value` column.

        Returns:
            DataFrame: PySpark DataFrame containing only records classified as anomalies.
                Includes additional columns:
                - `mad_zscore`: Computed MAD-based anomaly score.
                - `is_anomaly`: Boolean anomaly flag.
        """

        pdf = df.toPandas()

        scores = self.scorer.score(pdf["value"])
        pdf["mad_zscore"] = scores
        pdf["is_anomaly"] = self.scorer.is_anomaly(scores)

        return df.sparkSession.createDataFrame(pdf[pdf["is_anomaly"]].copy())


class DecompositionMadAnomalyDetection(AnomalyDetectionInterface):
    """
    Detects anomalies using time series decomposition followed by MAD scoring on residuals.

    This anomaly detection component combines seasonal-trend decomposition with robust
    Median Absolute Deviation (MAD) scoring:

    1) Decompose the input time series to remove trend and seasonality (STL or MSTL)
    2) Compute MAD-based anomaly scores on the `residual` component
    3) Return only rows flagged as anomalies

    The decomposition step helps isolate irregular behavior by removing structured
    components (trend/seasonality), which typically improves anomaly detection quality
    on periodic or drifting signals.

    This component takes a PySpark DataFrame as input and returns a PySpark DataFrame.
    Internally, the decomposed DataFrame is converted to Pandas for scoring.

    Example
    -------
    ```python
    from pyspark.sql import SparkSession
    from rtdip_sdk.pipelines.anomaly_detection.mad import (
        DecompositionMadAnomalyDetection,
        GlobalMadScorer,
    )

    spark = SparkSession.builder.getOrCreate()

    spark_df = spark.createDataFrame(
        [
            ("2024-01-01 00:00:00", 10.0, "sensor_a"),
            ("2024-01-01 01:00:00", 11.0, "sensor_a"),
            ("2024-01-01 02:00:00", 500.0, "sensor_a"),
            ("2024-01-01 03:00:00", 12.0, "sensor_a"),
        ],
        ["timestamp", "value", "sensor"],
    )

    detector = DecompositionMadAnomalyDetection(
        scorer=GlobalMadScorer(),
        decomposition="mstl",
        period=24,
        group_columns=["sensor"],
        timestamp_column="timestamp",
        value_column="value",
    )

    anomalies_df = detector.detect(spark_df)
    anomalies_df.show()
    ```

    Parameters:
        scorer (MadScorer): MAD-based scoring strategy used to compute anomaly scores
            on the decomposition residuals (e.g., `GlobalMadScorer`, `RollingMadScorer`).
        decomposition (str): Decomposition method to apply. Supported values are
            `'stl'` and `'mstl'`. Defaults to `'mstl'`.
        period (Union[int, str]): Seasonal period configuration passed to the
            decomposition component. Can be an integer (e.g., 24) or a period string
            depending on the decomposition implementation. Defaults to 24.
        group_columns (Optional[List[str]]): Columns defining separate time series
            groups (e.g., `['sensor_id']`). If provided, decomposition is performed
            separately per group. Defaults to None.
        timestamp_column (str): Name of the timestamp column. Defaults to `"timestamp"`.
        value_column (str): Name of the value column. Defaults to `"value"`.
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
        Applies the configured decomposition method (STL or MSTL) to the input DataFrame.

        Parameters:
            df (DataFrame): Input PySpark DataFrame containing the time series data.

        Returns:
            DataFrame: Decomposed PySpark DataFrame expected to include a `residual` column.

        Raises:
            ValueError: If `self.decomposition` is not one of `'stl'` or `'mstl'`.
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
        """
        Detects anomalies by scoring the decomposition residuals using the configured MAD scorer.

        The method decomposes the input series, computes MAD-based scores on the `residual`
        column, and returns only rows classified as anomalies.

        Parameters:
            df (DataFrame): Input PySpark DataFrame containing the time series data.

        Returns:
            DataFrame: PySpark DataFrame containing only records classified as anomalies.
                Includes additional columns:
                - `residual`: Residual component produced by the decomposition step.
                - `mad_zscore`: MAD-based anomaly score computed on `residual`.
                - `is_anomaly`: Boolean anomaly flag.
        """
        
        decomposed_df = self._decompose(df)
        pdf = decomposed_df.toPandas().sort_values(self.timestamp_column)

        scores = self.scorer.score(pdf["residual"])
        pdf["mad_zscore"] = scores
        pdf["is_anomaly"] = self.scorer.is_anomaly(scores)

        return df.sparkSession.createDataFrame(pdf[pdf["is_anomaly"]].copy())
