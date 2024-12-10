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
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)
from functools import reduce
from operator import or_

from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.interfaces import (
    MonitoringBaseInterface,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from ...input_validator import InputValidator


class CheckValueRanges(MonitoringBaseInterface, InputValidator):
    """
    Monitors data in a DataFrame by checking the 'Value' column against expected ranges for specified TagNames.
    Logs events when 'Value' exceeds the defined ranges for any TagName.

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
        from rtdip_sdk.pipelines.monitoring.spark.data_quality.check_value_ranges import CheckValueRanges

        spark = SparkSession.builder.master("local[1]").appName("CheckValueRangesExample").getOrCreate()

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

        check_value_ranges = CheckValueRanges(
            df=df,
            tag_ranges=tag_ranges,
        )

        result_df = check_value_ranges.check()
        ```
    """

    df: PySparkDataFrame
    tag_ranges: dict
    EXPECTED_SCHEMA = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", FloatType(), True),
        ]
    )

    def __init__(
        self,
        df: PySparkDataFrame,
        tag_ranges: dict,
    ) -> None:
        self.df = df
        self.validate(self.EXPECTED_SCHEMA)
        self.tag_ranges = tag_ranges

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

    def check(self) -> PySparkDataFrame:
        """
        Executes the value range checking logic for the specified TagNames. Identifies and logs any rows
        where 'Value' exceeds the defined ranges for each TagName.

        Returns:
            pyspark.sql.DataFrame:
                Returns the original PySpark DataFrame without changes.
        """
        self._validate_inputs()

        for tag_name, range_dict in self.tag_ranges.items():
            df = self.df.filter(col("TagName") == tag_name)

            if df.count() == 0:
                self.logger.warning(f"No data found for TagName '{tag_name}'.")
                continue

            min_value = range_dict.get("min", None)
            max_value = range_dict.get("max", None)
            inclusive_bounds = range_dict.get("inclusive_bounds", True)

            conditions = []

            # Build minimum value condition
            if min_value is not None:
                if inclusive_bounds:
                    min_condition = col("Value") < min_value
                else:
                    min_condition = col("Value") <= min_value
                conditions.append(min_condition)

            # Build maximum value condition
            if max_value is not None:
                if inclusive_bounds:
                    max_condition = col("Value") > max_value
                else:
                    max_condition = col("Value") >= max_value
                conditions.append(max_condition)

            if not conditions:
                continue

            condition = reduce(or_, conditions)
            out_of_range_df = df.filter(condition)

            count = out_of_range_df.count()
            if count > 0:
                self.logger.info(
                    f"Found {count} rows in 'Value' column for TagName '{tag_name}' out of range."
                )
                out_of_range_rows = out_of_range_df.collect()
                for row in out_of_range_rows:
                    self.logger.info(
                        f"Out of range row for TagName '{tag_name}': {row}"
                    )
            else:
                self.logger.info(
                    f"No out of range values found in 'Value' column for TagName '{tag_name}'."
                )

        return self.df

    def _validate_inputs(self):
        if not isinstance(self.tag_ranges, dict):
            raise TypeError("tag_ranges must be a dictionary.")

        # Erstelle eine Liste aller verfügbaren TagNames im DataFrame
        available_tags = (
            self.df.select("TagName").distinct().rdd.map(lambda row: row[0]).collect()
        )

        for tag_name, range_dict in self.tag_ranges.items():
            # Überprüfung, ob der TagName ein gültiger String ist
            if not isinstance(tag_name, str):
                raise ValueError(f"TagName '{tag_name}' must be a string.")

            # Überprüfung, ob der TagName im DataFrame existiert
            if tag_name not in available_tags:
                raise ValueError(f"TagName '{tag_name}' not found in DataFrame.")

            # Überprüfung, ob min und/oder max angegeben sind
            if "min" not in range_dict and "max" not in range_dict:
                raise ValueError(
                    f"TagName '{tag_name}' must have at least 'min' or 'max' specified."
                )

            # Überprüfung, ob inclusive_bounds ein boolescher Wert ist
            inclusive_bounds = range_dict.get("inclusive_bounds", True)
            if not isinstance(inclusive_bounds, bool):
                raise ValueError(
                    f"Inclusive_bounds for TagName '{tag_name}' must be a boolean."
                )

            # Optionale Überprüfung, ob min und max numerisch sind
            min_value = range_dict.get("min", None)
            max_value = range_dict.get("max", None)
            if min_value is not None and not isinstance(min_value, (int, float)):
                raise ValueError(
                    f"Minimum value for TagName '{tag_name}' must be a number."
                )
            if max_value is not None and not isinstance(max_value, (int, float)):
                raise ValueError(
                    f"Maximum value for TagName '{tag_name}' must be a number."
                )
