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
from pyspark.sql.functions import col, when, lag, count, sum
from pyspark.sql.window import Window

from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.interfaces import (
    MonitoringBaseInterface,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class FlatlineDetection(MonitoringBaseInterface):
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
        from rtdip_sdk.pipelines.monitoring.spark.data_manipulation.flatline_detection import FlatlineDetection
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

    def __init__(
        self, df: PySparkDataFrame, watch_columns: list, tolerance_timespan: int
    ) -> None:
        if not watch_columns or not isinstance(watch_columns, list):
            raise ValueError("watch_columns must be a non-empty list of column names.")
        if not isinstance(tolerance_timespan, int) or tolerance_timespan <= 0:
            raise ValueError("tolerance_timespan must be a positive integer.")

        self.df = df
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
        Detects flatlining in the specified columns and logs warnings if detected.

        Returns:
            pyspark.sql.DataFrame: The original PySpark DataFrame unchanged.
        """
        sort_column = self.df.columns[0]

        for column in self.watch_columns:
            # Flag null or zero values
            flagged_column = f"{column}_flatline_flag"
            flagged_df = self.df.withColumn(
                flagged_column,
                when((col(column).isNull()) | (col(column) == 0), 1).otherwise(0),
            )

            # Create a group for consecutive flatline streaks
            group_column = f"{column}_group"
            flagged_df = flagged_df.withColumn(
                group_column,
                (
                    col(flagged_column)
                    != lag(col(flagged_column), 1, 0).over(Window.orderBy(sort_column))
                ).cast("int"),
            )
            flagged_df = flagged_df.withColumn(
                group_column, sum(col(group_column)).over(Window.orderBy(sort_column))
            )

            # Count rows in each group
            group_counts = (
                flagged_df.filter(col(flagged_column) == 1)
                .groupBy(group_column)
                .count()
            )

            # Filter groups that exceed the tolerance
            large_groups = group_counts.filter(col("count") > self.tolerance_timespan)

            # Log all rows in groups exceeding tolerance
            if large_groups.count() > 0:
                large_group_ids = [row[group_column] for row in large_groups.collect()]
                relevant_rows = (
                    flagged_df.filter(col(group_column).isin(large_group_ids))
                    .select(*self.df.columns)
                    .collect()
                )
                for row in relevant_rows:
                    self.logger.warning(
                        f"Flatlining detected in column '{column}' at row: {row}."
                    )
            else:
                self.logger.info(f"No flatlining detected in column '{column}'.")

        return self.df
