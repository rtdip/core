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
import logging
from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql.functions import col, when, lag, sum, lit, abs
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)

from ..interfaces import MonitoringBaseInterface
from ...._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from ...input_validator import InputValidator


class FlatlineDetection(MonitoringBaseInterface, InputValidator):
    """
    Detects flatlining in specified columns of a PySpark DataFrame and logs warnings.

    Flatlining occurs when a column contains consecutive null or zero values exceeding a specified tolerance period.
    This class identifies such occurrences and logs the rows where flatlining is detected.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to monitor for flatlining.
        watch_columns (list): List of column names to monitor for flatlining (null or zero values).
        tolerance_timespan (int): Maximum allowed consecutive flatlining period. If exceeded, a warning is logged.

    Example:
        ```python
        from rtdip_sdk.pipelines.data_quality.monitoring.spark.flatline_detection import FlatlineDetection

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.master("local[1]").appName("FlatlineDetectionExample").getOrCreate()

        # Example DataFrame
        data = [
            (1, 1),
            (2, 0),
            (3, 0),
            (4, 0),
            (5, 5),
        ]
        columns = ["ID", "Value"]
        df = spark.createDataFrame(data, columns)

        # Initialize FlatlineDetection
        flatline_detection = FlatlineDetection(
            df,
            watch_columns=["Value"],
            tolerance_timespan=2
        )

        # Detect flatlining
        flatline_detection.check()
        ```
    """

    df: PySparkDataFrame
    watch_columns: list
    tolerance_timespan: int
    EXPECTED_SCHEMA = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    def __init__(
        self, df: PySparkDataFrame, watch_columns: list, tolerance_timespan: int
    ) -> None:
        if not watch_columns or not isinstance(watch_columns, list):
            raise ValueError("watch_columns must be a non-empty list of column names.")
        if not isinstance(tolerance_timespan, int) or tolerance_timespan <= 0:
            raise ValueError("tolerance_timespan must be a positive integer.")

        self.df = df
        self.validate(self.EXPECTED_SCHEMA)
        self.watch_columns = watch_columns
        self.tolerance_timespan = tolerance_timespan

        self.logger = logging.getLogger(self.__class__.__name__)
        if not self.logger.handlers:
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
        Detects flatlining and logs relevant rows.

        Returns:
            pyspark.sql.DataFrame: The original DataFrame with additional flatline detection metadata.
        """
        flatlined_rows = self.check_for_flatlining()
        print("Flatlined Rows:")
        flatlined_rows.show(truncate=False)
        self.log_flatlining_rows(flatlined_rows)
        return self.df

    def check_for_flatlining(self) -> PySparkDataFrame:
        """
        Identifies rows with flatlining based on the specified columns and tolerance.

        Returns:
            pyspark.sql.DataFrame: A DataFrame containing rows with flatlining detected.
        """
        partition_column = "TagName"
        sort_column = "EventTime"
        window_spec = Window.partitionBy(partition_column).orderBy(sort_column)

        # Start with an empty DataFrame, ensure it has the required schema
        flatlined_rows = (
            self.df.withColumn("Value_flatline_flag", lit(None).cast("int"))
            .withColumn("Value_group", lit(None).cast("bigint"))
            .filter("1=0")
        )

        for column in self.watch_columns:
            flagged_column = f"{column}_flatline_flag"
            group_column = f"{column}_group"

            # Add flag and group columns
            df_with_flags = self.df.withColumn(
                flagged_column,
                when(
                    (col(column).isNull()) | (abs(col(column) - 0.0) <= 1e-09),
                    1,
                ).otherwise(0),
            ).withColumn(
                group_column,
                sum(
                    when(
                        col(flagged_column)
                        != lag(col(flagged_column), 1, 0).over(window_spec),
                        1,
                    ).otherwise(0)
                ).over(window_spec),
            )

            # Identify flatlining groups
            group_counts = (
                df_with_flags.filter(col(flagged_column) == 1)
                .groupBy(group_column)
                .count()
            )
            large_groups = group_counts.filter(col("count") > self.tolerance_timespan)
            large_group_ids = [row[group_column] for row in large_groups.collect()]

            if large_group_ids:
                relevant_rows = df_with_flags.filter(
                    col(group_column).isin(large_group_ids)
                )

                # Ensure both DataFrames have the same columns
                for col_name in flatlined_rows.columns:
                    if col_name not in relevant_rows.columns:
                        relevant_rows = relevant_rows.withColumn(col_name, lit(None))

                flatlined_rows = flatlined_rows.union(relevant_rows)

        return flatlined_rows

    def log_flatlining_rows(self, flatlined_rows: PySparkDataFrame):
        """
        Logs flatlining rows for all monitored columns.

        Args:
            flatlined_rows (pyspark.sql.DataFrame): The DataFrame containing rows with flatlining detected.
        """
        if flatlined_rows.count() == 0:
            self.logger.info("No flatlining detected.")
            return

        for column in self.watch_columns:
            flagged_column = f"{column}_flatline_flag"

            if flagged_column not in flatlined_rows.columns:
                self.logger.warning(
                    f"Expected column '{flagged_column}' not found in DataFrame."
                )
                continue

            relevant_rows = flatlined_rows.filter(col(flagged_column) == 1).collect()

            if relevant_rows:
                for row in relevant_rows:
                    self.logger.warning(
                        f"Flatlining detected in column '{column}' at row: {row}."
                    )
            else:
                self.logger.info(f"No flatlining detected in column '{column}'.")
