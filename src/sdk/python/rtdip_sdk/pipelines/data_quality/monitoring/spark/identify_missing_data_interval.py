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

import logging

from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.interfaces import (
    MonitoringBaseInterface,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.time_string_parsing import (
    parse_time_string_to_ms,
)


class IdentifyMissingDataInterval(MonitoringBaseInterface):
    """
    Detects missing data intervals in a DataFrame by identifying time differences between consecutive
    measurements that exceed a specified tolerance or a multiple of the Median Absolute Deviation (MAD).
    Logs the start and end times of missing intervals along with their durations.


    Args:
        df (pyspark.sql.Dataframe): DataFrame containing at least the 'EventTime' column.
        interval (str, optional): Expected interval between data points (e.g., '10ms', '500ms'). If not specified, the median of time differences is used.
        tolerance (str, optional): Tolerance time beyond which an interval is considered missing (e.g., '10ms'). If not specified, it defaults to 'mad_multiplier' times the Median Absolute Deviation (MAD) of time differences.
        mad_multiplier (float, optional): Multiplier for MAD to calculate tolerance. Default is 3.
        min_tolerance (str, optional): Minimum tolerance for pattern-based detection (e.g., '100ms'). Default is '10ms'.

    Returns:
        df (pyspark.sql.Dataframe): Returns the original PySparkDataFrame without changes.

    Example
    --------
    ```python
      from rtdip_sdk.pipelines.monitoring.spark.data_manipulation import IdentifyMissingDataInterval
    from pyspark.sql import SparkSession

    missing_data_monitor = IdentifyMissingDataInterval(
        df=df,
        interval='100ms',
        tolerance='10ms',
    )

    df_result = missing_data_monitor.check()

    """

    df: PySparkDataFrame

    def __init__(
        self,
        df: PySparkDataFrame,
        interval: str = None,
        tolerance: str = None,
        mad_multiplier: float = 3,
        min_tolerance: str = "10ms",
    ) -> None:

        self.df = df
        self.interval = interval
        self.tolerance = tolerance
        self.mad_multiplier = mad_multiplier
        self.min_tolerance = min_tolerance

        # Configure logging
        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
            # Prevent adding multiple handlers in interactive environments
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

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

    def check(self) -> PySparkDataFrame:
        if "EventTime" not in self.df.columns:
            self.logger.error("The DataFrame must contain an 'EventTime' column.")
            raise ValueError("The DataFrame must contain an 'EventTime' column.")

        df = self.df.withColumn("EventTime", F.to_timestamp("EventTime"))
        df_sorted = df.orderBy("EventTime")
        # Calculate time difference in milliseconds between consecutive rows
        df_with_diff = df_sorted.withColumn(
            "TimeDeltaMs",
            (
                F.col("EventTime").cast("double")
                - F.lag("EventTime").over(Window.orderBy("EventTime")).cast("double")
            )
            * 1000,
        ).withColumn(
            "StartMissing", F.lag("EventTime").over(Window.orderBy("EventTime"))
        )
        # Parse interval to milliseconds if given
        if self.interval is not None:
            try:
                interval_ms = parse_time_string_to_ms(self.interval)
                self.logger.info(f"Using provided expected interval: {interval_ms} ms")
            except ValueError as e:
                self.logger.error(e)
                raise
        else:
            # Calculate interval based on median of time differences
            median_expr = F.expr("percentile_approx(TimeDeltaMs, 0.5)")
            median_row = df_with_diff.select(median_expr.alias("median")).collect()[0]
            interval_ms = median_row["median"]
            self.logger.info(
                f"Using median of time differences as expected interval: {interval_ms} ms"
            )
        # Parse tolernace to milliseconds if given
        if self.tolerance is not None:
            try:
                tolerance_ms = parse_time_string_to_ms(self.tolerance)
                self.logger.info(f"Using provided tolerance: {tolerance_ms} ms")
            except ValueError as e:
                self.logger.error(e)
                raise
        else:
            # Calulate tolerance based on MAD
            mad_expr = F.expr(
                f"percentile_approx(abs(TimeDeltaMs - {interval_ms}), 0.5)"
            )
            mad_row = df_with_diff.select(mad_expr.alias("mad")).collect()[0]
            mad = mad_row["mad"]
            calculated_tolerance_ms = self.mad_multiplier * mad
            min_tolerance_ms = parse_time_string_to_ms(self.min_tolerance)
            tolerance_ms = max(calculated_tolerance_ms, min_tolerance_ms)
            self.logger.info(f"Calculated tolerance: {tolerance_ms} ms (MAD-based)")
        # Calculate the maximum acceptable interval with tolerance
        max_interval_with_tolerance_ms = interval_ms + tolerance_ms
        self.logger.info(
            f"Maximum acceptable interval with tolerance: {max_interval_with_tolerance_ms} ms"
        )

        # Identify missing intervals
        missing_intervals_df = df_with_diff.filter(
            (F.col("TimeDeltaMs") > max_interval_with_tolerance_ms)
            & (F.col("StartMissing").isNotNull())
        ).select("StartMissing", F.col("EventTime").alias("EndMissing"), "TimeDeltaMs")
        # Convert time delta to readable format
        missing_intervals_df = missing_intervals_df.withColumn(
            "DurationMissing",
            F.concat(
                F.floor(F.col("TimeDeltaMs") / 3600000).cast("string"),
                F.lit("h "),
                F.floor((F.col("TimeDeltaMs") % 3600000) / 60000).cast("string"),
                F.lit("m "),
                F.floor(((F.col("TimeDeltaMs") % 3600000) % 60000) / 1000).cast(
                    "string"
                ),
                F.lit("s"),
            ),
        ).select("StartMissing", "EndMissing", "DurationMissing")
        missing_intervals = missing_intervals_df.collect()
        if missing_intervals:
            self.logger.info("Detected Missing Intervals:")
            for row in missing_intervals:
                self.logger.info(
                    f"Missing Interval from {row['StartMissing']} to {row['EndMissing']} "
                    f"Duration: {row['DurationMissing']}"
                )
        else:
            self.logger.info("No missing intervals detected.")
        return self.df
