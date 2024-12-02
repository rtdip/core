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
    Monitors data in a DataFrame by checking specified columns against expected value ranges.
    Logs events when values exceed the specified ranges.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to monitor.
        columns_ranges (dict): A dictionary where keys are column names and values are dictionaries specifying 'min' and/or
            'max', and optionally 'inclusive_bounds' values.
            Example:
                {
                    'temperature': {'min': 0, 'max': 100, 'inclusive_bounds': True},
                    'pressure': {'min': 10, 'max': 200, 'inclusive_bounds': False},
                    'humidity': {'min': 30}  # Defaults to inclusive_bounds = True
                }

    Example:
        ```python
        from pyspark.sql import SparkSession
        from rtdip_sdk.pipelines.monitoring.spark.data_quality.check_value_ranges import CheckValueRanges

        spark = SparkSession.builder.master("local[1]").appName("CheckValueRangesExample").getOrCreate()

        data = [
            (1, 25, 100),
            (2, -5, 150),
            (3, 50, 250),
            (4, 80, 300),
            (5, 100, 50),
        ]

        columns = ["ID", "temperature", "pressure"]

        df = spark.createDataFrame(data, columns)

        columns_ranges = {
            "temperature": {"min": 0, "max": 100, "inclusive_bounds": False},
            "pressure": {"min": 50, "max": 200},
        }

        check_value_ranges = CheckValueRanges(
            df=df,
            columns_ranges=columns_ranges,
        )

        result_df = check_value_ranges.check()
        ```
    """

    df: PySparkDataFrame

    def __init__(
        self,
        df: PySparkDataFrame,
        columns_ranges: dict,
    ) -> None:

        self.df = df
        self.columns_ranges = columns_ranges

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
        Executes the value range checking logic. Identifies and logs any rows where specified
        columns exceed their defined value ranges.

        Returns:
            pyspark.sql.DataFrame:
                Returns the original PySpark DataFrame without changes.
        """
        self._validate_inputs()
        df = self.df

        for column, range_dict in self.columns_ranges.items():
            min_value = range_dict.get("min", None)
            max_value = range_dict.get("max", None)
            inclusive_bounds = range_dict.get("inclusive_bounds", True)

            conditions = []

            # Build minimum value condition
            if min_value is not None:
                if inclusive_bounds:
                    min_condition = col(column) < min_value
                else:
                    min_condition = col(column) <= min_value
                conditions.append(min_condition)

            # Build maximum value condition
            if max_value is not None:
                if inclusive_bounds:
                    max_condition = col(column) > max_value
                else:
                    max_condition = col(column) >= max_value
                conditions.append(max_condition)

            if not conditions:
                continue

            condition = reduce(or_, conditions)
            out_of_range_df = df.filter(condition)

            count = out_of_range_df.count()
            if count > 0:
                self.logger.info(
                    f"Found {count} rows in column '{column}' out of range."
                )
                out_of_range_rows = out_of_range_df.collect()
                for row in out_of_range_rows:
                    self.logger.info(f"Out of range row in column '{column}': {row}")
            else:
                self.logger.info(f"No out of range values found in column '{column}'.")

        return self.df

    def _validate_inputs(self):
        if not isinstance(self.columns_ranges, dict):
            raise TypeError("columns_ranges must be a dictionary.")

        for column, range_dict in self.columns_ranges.items():
            if column not in self.df.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame.")

            inclusive_bounds = range_dict.get("inclusive_bounds", True)
            if not isinstance(inclusive_bounds, bool):
                raise ValueError(
                    f"Inclusive_bounds for column '{column}' must be a boolean."
                )

            if "min" not in range_dict and "max" not in range_dict:
                raise ValueError(
                    f"Column '{column}' must have at least 'min' or 'max' specified."
                )
