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
from datetime import timedelta

import pandas as pd
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from ..interfaces import DataManipulationBaseInterface
from ...input_validator import InputValidator


class IntervalFiltering(DataManipulationBaseInterface, InputValidator):
    """
    Cleanses a DataFrame by removing rows outside a specified interval window. Supported time stamp columns are DateType and StringType.

    Parameters:
        spark (SparkSession): A SparkSession object.
        df (DataFrame): PySpark DataFrame to be converted
        interval (int): The interval length for cleansing.
        interval_unit (str): 'hours', 'minutes', 'seconds' or 'milliseconds' to specify the unit of the interval.
        time_stamp_column_name (str): The name of the column containing the time stamps. Default is 'EventTime'.
        tolerance (int): The tolerance for the interval. Default is None.
    """

    """ Default time stamp column name if not set in the constructor """
    DEFAULT_TIME_STAMP_COLUMN_NAME: str = "EventTime"

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        interval: int,
        interval_unit: str,
        time_stamp_column_name: str = None,
        tolerance: int = None,
    ) -> None:
        self.spark = spark
        self.df = df
        self.interval = interval
        self.interval_unit = interval_unit
        self.tolerance = tolerance
        if time_stamp_column_name is None:
            self.time_stamp_column_name = self.DEFAULT_TIME_STAMP_COLUMN_NAME
        else:
            self.time_stamp_column_name = time_stamp_column_name

    def filter(self) -> DataFrame:
        """
        Filters the DataFrame based on the interval
        """

        if self.time_stamp_column_name not in self.df.columns:
            raise ValueError(
                f"Column {self.time_stamp_column_name} not found in the DataFrame."
            )
        is_string_time_stamp = isinstance(
            self.df.schema[self.time_stamp_column_name].dataType, StringType
        )

        original_schema = self.df.schema
        self.df = self.convert_column_to_timestamp().orderBy(
            self.time_stamp_column_name
        )

        tolerance_in_ms = None
        if self.tolerance is not None:
            tolerance_in_ms = self.get_time_delta(self.tolerance).total_seconds() * 1000

        time_delta_in_ms = self.get_time_delta(self.interval).total_seconds() * 1000

        rows = self.df.collect()
        last_time_stamp = rows[0][self.time_stamp_column_name]
        first_row = rows[0].asDict()

        first_row[self.time_stamp_column_name] = (
            self.format_date_time_to_string(first_row[self.time_stamp_column_name])
            if is_string_time_stamp
            else first_row[self.time_stamp_column_name]
        )

        cleansed_df = [first_row]

        for i in range(1, len(rows)):
            current_row = rows[i]
            current_time_stamp = current_row[self.time_stamp_column_name]

            if self.check_outside_of_interval(
                current_time_stamp, last_time_stamp, time_delta_in_ms, tolerance_in_ms
            ):
                current_row_dict = current_row.asDict()
                current_row_dict[self.time_stamp_column_name] = (
                    self.format_date_time_to_string(
                        current_row_dict[self.time_stamp_column_name]
                    )
                    if is_string_time_stamp
                    else current_row_dict[self.time_stamp_column_name]
                )

                cleansed_df.append(current_row_dict)
                last_time_stamp = current_time_stamp

        result_df = self.spark.createDataFrame(cleansed_df, schema=original_schema)

        return result_df

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

    def convert_column_to_timestamp(self) -> DataFrame:
        try:
            return self.df.withColumn(
                self.time_stamp_column_name, F.to_timestamp(self.time_stamp_column_name)
            )
        except Exception as e:
            raise ValueError(
                f"Error converting column {self.time_stamp_column_name} to timestamp: {e}"
                f"{self.df.schema[self.time_stamp_column_name].dataType} might be unsupported!"
            )

    def get_time_delta(self, value: int) -> timedelta:
        if self.interval_unit == "minutes":
            return timedelta(minutes=value)
        elif self.interval_unit == "days":
            return timedelta(days=value)
        elif self.interval_unit == "hours":
            return timedelta(hours=value)
        elif self.interval_unit == "seconds":
            return timedelta(seconds=value)
        elif self.interval_unit == "milliseconds":
            return timedelta(milliseconds=value)
        else:
            raise ValueError(
                "interval_unit must be either 'days', 'hours', 'minutes', 'seconds' or 'milliseconds'"
            )

    def check_outside_of_interval(
        self,
        current_time_stamp: pd.Timestamp,
        last_time_stamp: pd.Timestamp,
        time_delta_in_ms: float,
        tolerance_in_ms: float,
    ) -> bool:
        time_difference = (current_time_stamp - last_time_stamp).total_seconds() * 1000
        if not tolerance_in_ms is None:
            time_difference += tolerance_in_ms
        return time_difference >= time_delta_in_ms

    def format_date_time_to_string(self, time_stamp: pd.Timestamp) -> str:
        try:
            return time_stamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        except Exception as e:
            raise ValueError(f"Error converting timestamp to string: {e}")
