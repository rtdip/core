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

from pyspark.sql import DataFrame as PySparkDataFrame, functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, TimestampType, FloatType, ArrayType
from pyspark.sql.window import Window
from datetime import timedelta
from typing import List
from ...interfaces import WranglerBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class MissingValueImputation(WranglerBaseInterface):
    """
    TODO

    Args:
        TODO

    Returns:
        TODO

    Example
    --------
    TODO
    """


    df: PySparkDataFrame

    def __init__(
        self,
        df: PySparkDataFrame
    ) -> None:
        self.df = df

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


    def filter(self) -> PySparkDataFrame:
        """
        Imputate missing values based on []
        """

        if not all(col_ in self.df.columns for col_ in ["TagName", "EventTime", "Value"]):
            raise ValueError("Columns not as expected")

        if self._is_column_type(self.df, "EventTime", StringType):
            self.df = self.df.withColumn("EventTime", F.to_timestamp("EventTime"))
        if self._is_column_type(self.df, "Value", StringType):
            self.df = self.df.withColumn("Value", self.df["Value"].cast(FloatType()))

        dfs_by_source = self._split_by_source()

        flagged_dfs: List[PySparkDataFrame] = []

        for source, df in dfs_by_source.items():
            flagged_df = self._flag_missing_values(df)

            print(flagged_df.show(truncate=False)) # Current testing
            flagged_dfs.append(flagged_df)

        return self.df


    def _flag_missing_values(self, df) -> PySparkDataFrame:
        window_spec = Window.partitionBy("TagName").orderBy("EventTime")

        df = df.withColumn("prev_event_time", F.lag("EventTime").over(window_spec))
        df = df.withColumn("time_diff_seconds",
                           (F.unix_timestamp("EventTime") - F.unix_timestamp("prev_event_time")))

        df_diff = df.filter(F.col("time_diff_seconds").isNotNull())
        interval_counts = df_diff.groupBy("time_diff_seconds").count()
        most_frequent_interval = interval_counts.orderBy(F.desc("count")).first()
        expected_interval = most_frequent_interval["time_diff_seconds"] if most_frequent_interval else None

        tolerance_percentage = 15
        tolerance = (expected_interval * tolerance_percentage) / 100 if expected_interval else 0

        existing_timestamps = df.select("TagName", "EventTime").rdd \
            .map(lambda row: (row["TagName"], row["EventTime"])).groupByKey().collectAsMap()

        def generate_missing_timestamps(prev_event_time, event_time, tag_name):
            # Check for first row
            if prev_event_time is None or event_time is None or expected_interval is None:
                return []

            # Check against existing timestamps to avoid duplicates
            tag_timestamps = set(existing_timestamps.get(tag_name, []))
            missing_timestamps = []
            current_time = prev_event_time

            while current_time < event_time:
                next_expected_time = current_time + timedelta(seconds=expected_interval)
                time_diff = abs((next_expected_time - event_time).total_seconds())
                if time_diff <= tolerance:
                    break
                if next_expected_time not in tag_timestamps:
                    missing_timestamps.append(next_expected_time)
                current_time = next_expected_time

            return missing_timestamps

        generate_missing_timestamps_udf = udf(generate_missing_timestamps, ArrayType(TimestampType()))

        df_with_missing = df.withColumn(
            "missing_timestamps",
            generate_missing_timestamps_udf("prev_event_time", "EventTime", "TagName")
        )

        df_missing_entries = df_with_missing.select(
            "TagName",
            F.explode("missing_timestamps").alias("EventTime"),
            F.lit("Good").alias("Status"),
            F.lit(float('nan')).cast(FloatType()).alias("Value")
        )

        df_combined = df.select("TagName", "EventTime", "Status", "Value").union(df_missing_entries).orderBy(
            "EventTime")

        return df_combined


    def _split_by_source(self) -> dict:
        #
        tag_names = self.df.select("TagName").distinct().collect()
        tag_names = [row["TagName"] for row in tag_names]
        source_dict = {tag: self.df.filter(col("TagName") == tag).orderBy("EventTime") for tag in tag_names}

        return source_dict


    def _is_column_type(self, df, column_name, data_type):
        #
        type_ = df.schema[column_name]

        return isinstance(type_.dataType, data_type)
