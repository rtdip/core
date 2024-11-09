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
from databricks.sqlalchemy.test_local.conftest import schema
from pyspark.sql import functions as F
from pyspark.sql import  SparkSession
from pyspark.sql import DataFrame

from ...._pipeline_utils.models import Libraries, SystemType
from ...interfaces import WranglerBaseInterface

class IntervalFiltering(WranglerBaseInterface):
    """
       Cleanses a DataFrame by removing rows outside a specified interval window.
       Example:


       Parameters:
           spark (SparkSession): A SparkSession object.
           df (DataFrame): PySpark DataFrame to be converted
           interval (int): The interval length for cleansing.
            interval_unit (str): 'hours', 'minutes', 'seconds' or 'milliseconds' to specify the unit of the interval.
       """

    """ Default time stamp column name if not set in the constructor """
    DEFAULT_TIME_STAMP_COLUMN_NAME: str = "EventTime"


    def __init__(self, spark: SparkSession,  df: DataFrame, interval: int, interval_unit: str, time_stamp_column_name: str = None) -> None:
        self.spark = spark
        self.df = df
        self.interval = interval
        self.interval_unit = interval_unit
        if time_stamp_column_name is None:
            self.time_stamp_column_name = self.DEFAULT_TIME_STAMP_COLUMN_NAME
        else: self.time_stamp_column_name = time_stamp_column_name

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
            return self.df.withColumn(self.time_stamp_column_name, F.to_timestamp(self.time_stamp_column_name))
        except Exception as e:
            raise ValueError(f"Error converting column {self.time_stamp_column_name} to timestamp: {e}")

    def get_time_delta(self) -> timedelta:
        if self.interval_unit ==  'minutes':
            return timedelta(minutes = self.interval)
        elif self.interval_unit == 'days':
            return timedelta(days = self.interval)
        elif self.interval_unit == 'hours':
            return timedelta(hours = self.interval)
        elif self.interval_unit == 'seconds':
            return timedelta(seconds = self.interval)
        elif self.interval_unit == 'milliseconds':
            return timedelta(milliseconds = self.interval)
        else:
            raise ValueError("interval_unit must be either 'seconds' or 'milliseconds'")

    def format_date_time_to_string(self, time_stamp: pd.Timestamp) -> str:
        return time_stamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

    def filter(self) -> DataFrame:
        """
        Filters the DataFrame based on the interval
        """

        if self.time_stamp_column_name not in self.df.columns:
            raise ValueError(f"Column {self.time_stamp_column_name} not found in the DataFrame.")

        original_schema = self.df.schema
        self.df = self.convert_column_to_timestamp().orderBy(self.time_stamp_column_name)

        time_delta_in_ms = self.get_time_delta().total_seconds() * 1000

        rows = self.df.collect()
        last_time_stamp = rows[0][self.time_stamp_column_name]
        first_row = rows[0].asDict()
        first_row[self.time_stamp_column_name] = self.format_date_time_to_string(first_row[self.time_stamp_column_name])

        cleansed_df = [first_row]

        for i in range(1, len(rows)):
            current_row = rows[i]
            current_time_stamp = current_row[self.time_stamp_column_name]

            if ((current_time_stamp - last_time_stamp).total_seconds() * 1000) >= time_delta_in_ms:
                current_row_dict = current_row.asDict()
                current_row_dict[self.time_stamp_column_name] = self.format_date_time_to_string(current_row_dict[self.time_stamp_column_name])
                cleansed_df.append(current_row_dict)
                last_time_stamp = current_time_stamp

        result_df = self.spark.createDataFrame(cleansed_df, schema= original_schema)

        return result_df









