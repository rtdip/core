import logging

import pandas as pd
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import functions as F

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


class IdentifyMissingDataPattern(MonitoringBaseInterface):
    """
    Identifies missing data in a DataFrame based on specified time patterns.
    Logs the expected missing times.

    Args:
        df (pyspark.sql.Dataframe): DataFrame containing at least the 'EventTime' column.
        patterns (list of dict): List of dictionaries specifying the time patterns.
            - For 'minutely' frequency: Specify 'second' and optionally 'millisecond'.
              Example: [{'second': 0}, {'second': 13}, {'second': 49}]
            - For 'hourly' frequency: Specify 'minute', 'second', and optionally 'millisecond'.
              Example: [{'minute': 0, 'second': 0}, {'minute': 30, 'second': 30}]
        frequency (str): Frequency of the patterns. Must be either 'minutely' or 'hourly'.
            - 'minutely': Patterns are checked every minute at specified seconds.
            - 'hourly': Patterns are checked every hour at specified minutes and seconds.
        tolerance (str, optional): Maximum allowed deviation from the pattern (e.g., '1s', '500ms').
            Default is '10ms'.

    Returns:
        PySparkDataFrame: Returns the original PySpark DataFrame without changes.
    """

    df: PySparkDataFrame

    def __init__(
        self,
        df: PySparkDataFrame,
        patterns: list,
        frequency: str = "minutely",
        tolerance: str = "10ms",
    ) -> None:

        self.df = df
        self.patterns = patterns
        self.frequency = frequency.lower()
        self.tolerance = tolerance

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
        """
        Executes the missing pattern detection logic. Identifies and logs any missing patterns
        based on the provided patterns and frequency within the specified tolerance.

        Returns:
            pyspark.sql.DataFrame:
                Returns the original PySpark DataFrame without changes.
        """
        self._validate_inputs()
        df = self.df.withColumn("EventTime", F.to_timestamp("EventTime"))
        df_sorted = df.orderBy("EventTime")
        # Determine if the DataFrame is empty
        count = df_sorted.count()
        if count == 0:
            self.logger.info("Generated 0 expected times based on patterns.")
            self.logger.info("DataFrame is empty. No missing patterns to detect.")
            return self.df
        # Determine the time range of the data
        min_time, max_time = df_sorted.agg(
            F.min("EventTime"), F.max("EventTime")
        ).first()
        if not min_time or not max_time:
            self.logger.info("Generated 0 expected times based on patterns.")
            self.logger.info("DataFrame is empty. No missing patterns to detect.")
            return self.df
        # Generate all expected times based on patterns and frequency
        expected_times_df = self._generate_expected_times(min_time, max_time)
        # Identify missing patterns by left joining expected times with actual EventTimes within tolerance
        missing_patterns_df = self._find_missing_patterns(expected_times_df, df_sorted)
        self._log_missing_patterns(missing_patterns_df)
        return self.df

    def _validate_inputs(self):
        if self.frequency not in ["minutely", "hourly"]:
            error_msg = "Frequency must be either 'minutely' or 'hourly'."
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        for pattern in self.patterns:
            if self.frequency == "minutely":
                if "second" not in pattern:
                    raise ValueError(
                        "Each pattern must have a 'second' key for 'minutely' frequency."
                    )
                if pattern.get("second", 0) >= 60:
                    raise ValueError(
                        "For 'minutely' frequency, 'second' must be less than 60."
                    )
                if "minute" in pattern or "hour" in pattern:
                    raise ValueError(
                        "For 'minutely' frequency, pattern should not contain 'minute' or 'hour'."
                    )
            elif self.frequency == "hourly":
                if "minute" not in pattern or "second" not in pattern:
                    raise ValueError(
                        "Each pattern must have 'minute' and 'second' keys for 'hourly' frequency."
                    )
                if pattern.get("minute", 0) >= 60:
                    raise ValueError(
                        "For 'hourly' frequency, 'minute' must be less than 60."
                    )
                if "hour" in pattern:
                    raise ValueError(
                        "For 'hourly' frequency, pattern should not contain 'hour'."
                    )
        try:
            self.tolerance_ms = parse_time_string_to_ms(self.tolerance)
            self.tolerance_seconds = self.tolerance_ms / 1000
            self.logger.info(
                f"Using tolerance: {self.tolerance_ms} ms ({self.tolerance_seconds} seconds)"
            )
        except ValueError as e:
            error_msg = f"Invalid tolerance format: {self.tolerance}"
            self.logger.error(error_msg)
            raise ValueError(error_msg) from e

    def _generate_expected_times(self, min_time, max_time) -> PySparkDataFrame:
        """
        Generates all expected times based on the patterns and frequency within the time range.

        Args:
            min_time (Timestamp): Start of the time range.
            max_time (Timestamp): End of the time range.

        Returns:
            PySparkDataFrame: DataFrame containing all expected 'ExpectedTime'.
        """
        # Floor min_time to the nearest frequency step
        if self.frequency == "minutely":
            floor_min_time = min_time.replace(second=0, microsecond=0)
            step = F.expr("INTERVAL 1 MINUTE")
        elif self.frequency == "hourly":
            floor_min_time = min_time.replace(minute=0, second=0, microsecond=0)
            step = F.expr("INTERVAL 1 HOUR")
        # Ceil max_time to include the last interval
        if self.frequency == "minutely":
            ceil_max_time = (max_time + pd.Timedelta(minutes=1)).replace(
                second=0, microsecond=0
            )
        elif self.frequency == "hourly":
            ceil_max_time = (max_time + pd.Timedelta(hours=1)).replace(
                minute=0, second=0, microsecond=0
            )
        # Create a DataFrame with a sequence of base times
        base_times_df = self.df.sparkSession.createDataFrame(
            [(floor_min_time, ceil_max_time)], ["start", "end"]
        ).select(
            F.explode(
                F.sequence(
                    F.col("start").cast("timestamp"),
                    F.col("end").cast("timestamp"),
                    step,
                )
            ).alias("BaseTime")
        )
        # Generate expected times based on patterns
        expected_times = []
        for pattern in self.patterns:
            if self.frequency == "minutely":
                seconds = pattern.get("second", 0)
                milliseconds = pattern.get("millisecond", 0)
                expected_time = (
                    F.col("BaseTime")
                    + F.expr(f"INTERVAL {seconds} SECOND")
                    + F.expr(f"INTERVAL {milliseconds} MILLISECOND")
                )
            elif self.frequency == "hourly":
                minutes = pattern.get("minute", 0)
                seconds = pattern.get("second", 0)
                milliseconds = pattern.get("millisecond", 0)
                expected_time = (
                    F.col("BaseTime")
                    + F.expr(f"INTERVAL {minutes} MINUTE")
                    + F.expr(f"INTERVAL {seconds} SECOND")
                    + F.expr(f"INTERVAL {milliseconds} MILLISECOND")
                )
            expected_times.append(expected_time)
        # Combine all expected times into one DataFrame
        expected_times_df = base_times_df.withColumn(
            "ExpectedTime", F.explode(F.array(*expected_times))
        ).select("ExpectedTime")
        # Remove duplicates and filter within the time range
        expected_times_df = expected_times_df.distinct().filter(
            (F.col("ExpectedTime") >= F.lit(floor_min_time))
            & (F.col("ExpectedTime") <= F.lit(max_time))
        )
        self.logger.info(
            f"Generated {expected_times_df.count()} expected times based on patterns."
        )
        return expected_times_df

    def _find_missing_patterns(
        self, expected_times_df: PySparkDataFrame, actual_df: PySparkDataFrame
    ) -> PySparkDataFrame:
        """
        Finds missing patterns by comparing expected times with actual EventTimes within tolerance.

        Args:
            expected_times_df (PySparkDataFrame): DataFrame with expected 'ExpectedTime'.
            actual_df (PySparkDataFrame): Actual DataFrame with 'EventTime'.

        Returns:
            PySparkDataFrame: DataFrame with missing 'ExpectedTime'.
        """
        # Format tolerance for SQL INTERVAL
        tolerance_str = self._format_timedelta_for_sql(self.tolerance_ms)
        # Perform left join with tolerance window
        missing_patterns_df = (
            expected_times_df.alias("et")
            .join(
                actual_df.alias("at"),
                (
                    F.col("at.EventTime")
                    >= F.expr(f"et.ExpectedTime - INTERVAL {tolerance_str}")
                )
                & (
                    F.col("at.EventTime")
                    <= F.expr(f"et.ExpectedTime + INTERVAL {tolerance_str}")
                ),
                how="left",
            )
            .filter(F.col("at.EventTime").isNull())
            .select(F.col("et.ExpectedTime"))
        )
        self.logger.info(f"Identified {missing_patterns_df.count()} missing patterns.")
        return missing_patterns_df

    def _log_missing_patterns(self, missing_patterns_df: PySparkDataFrame):
        """
        Logs the missing patterns.

        Args:
            missing_patterns_df (PySparkDataFrame): DataFrame with missing 'ExpectedTime'.
        """
        missing_patterns = missing_patterns_df.collect()
        if missing_patterns:
            self.logger.info("Detected Missing Patterns:")
            # Sort missing patterns by ExpectedTime
            sorted_missing_patterns = sorted(
                missing_patterns, key=lambda row: row["ExpectedTime"]
            )
            for row in sorted_missing_patterns:
                # Format ExpectedTime to include milliseconds correctly
                formatted_time = row["ExpectedTime"].strftime("%Y-%m-%d %H:%M:%S.%f")[
                    :-3
                ]
                self.logger.info(f"Missing Pattern at {formatted_time}")
        else:
            self.logger.info("No missing patterns detected.")

    @staticmethod
    def _format_timedelta_for_sql(tolerance_ms: float) -> str:
        """
        Formats a tolerance in milliseconds to a string suitable for SQL INTERVAL.

        Args:
            tolerance_ms (float): Tolerance in milliseconds.

        Returns:
            str: Formatted string (e.g., '1 SECOND', '500 MILLISECONDS').
        """
        if tolerance_ms >= 3600000:
            hours = int(tolerance_ms // 3600000)
            return f"{hours} HOURS"
        elif tolerance_ms >= 60000:
            minutes = int(tolerance_ms // 60000)
            return f"{minutes} MINUTES"
        elif tolerance_ms >= 1000:
            seconds = int(tolerance_ms // 1000)
            return f"{seconds} SECONDS"
        else:
            milliseconds = int(tolerance_ms)
            return f"{milliseconds} MILLISECONDS"
