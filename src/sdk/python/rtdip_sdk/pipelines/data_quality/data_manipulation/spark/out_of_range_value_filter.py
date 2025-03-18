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

import logging
from pyspark.sql import DataFrame as PySparkDataFrame
from ...monitoring.spark.check_value_ranges import CheckValueRanges
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class OutOfRangeValueFilter(DataManipulationBaseInterface):
    """
    Filters data in a DataFrame by checking the 'Value' column against expected ranges for specified TagNames.
    Logs events when 'Value' exceeds the defined ranges for any TagName and deletes the rows.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to monitor.
        tag_ranges (dict): A dictionary where keys are TagNames and values are dictionaries specifying 'min' and/or
            'max', and optionally 'inclusive_bounds' values.
            Example:
                {
                    'A2PS64V0J.:ZUX09R': {'min': 0, 'max': 100, 'inclusive_bounds': True},
                    'B3TS64V0K.:ZUX09R': {'min': 10, 'max': 200, 'inclusive_bounds': False},
                }

    Example:
        ```python
        from pyspark.sql import SparkSession
        from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.out_of_range_value_filter import OutOfRangeValueFilter


        spark = SparkSession.builder.master("local[1]").appName("DeleteOutOfRangeValuesExample").getOrCreate()

        data = [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", 25.0),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", -5.0),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", 50.0),
            ("B3TS64V0K.:ZUX09R", "2024-01-02 16:00:12.000", "Good", 80.0),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", 100.0),
        ]

        columns = ["TagName", "EventTime", "Status", "Value"]

        df = spark.createDataFrame(data, columns)

        tag_ranges = {
            "A2PS64V0J.:ZUX09R": {"min": 0, "max": 50, "inclusive_bounds": True},
            "B3TS64V0K.:ZUX09R": {"min": 50, "max": 100, "inclusive_bounds": False},
        }

        out_of_range_value_filter = OutOfRangeValueFilter(
            df=df,
            tag_ranges=tag_ranges,
        )

        result_df = out_of_range_value_filter.filter_data()
        ```
    """

    df: PySparkDataFrame

    def __init__(
        self,
        df: PySparkDataFrame,
        tag_ranges: dict,
    ) -> None:
        self.df = df
        self.check_value_ranges = CheckValueRanges(df=df, tag_ranges=tag_ranges)

        # Configure logging
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

    def filter_data(self) -> PySparkDataFrame:
        """
        Executes the value range checking logic for the specified TagNames. Identifies, logs and deletes any rows
        where 'Value' exceeds the defined ranges for each TagName.

        Returns:
            pyspark.sql.DataFrame:
                Returns a PySpark DataFrame without the rows that were out of range.
        """
        out_of_range_df = self.check_value_ranges.check_for_out_of_range()

        if out_of_range_df.count() > 0:
            self.check_value_ranges.log_out_of_range_values(out_of_range_df)
        else:
            self.logger.info(f"No out of range values found in 'Value' column.")
        return self.df.subtract(out_of_range_df)
