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


class CheckValueRanges(MonitoringBaseInterface):
    """
    Monitors data in a DataFrame by checking specified columns against expected value ranges.
    Logs events when values exceed the specified ranges.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame to monitor.
        columns_ranges (dict): A dictionary where keys are column names and values are dictionaries specifying 'min' and/or
            'max', and optionally 'inclusive' values.
            Example:
                {
                    'temperature': {'min': 0, 'max': 100, 'inclusive': 'both'},
                    'pressure': {'min': 10, 'max': 200, 'inclusive': 'left'},
                    'humidity': {'min': 30}  # Uses default inclusive
                }
        default_inclusive (str, optional): Default inclusivity setting if not specified per column.
            Can be 'both', 'neither', 'left', or 'right'. Default is 'both'.
            - 'both': min <= value <= max
            - 'neither': min < value < max
            - 'left': min <= value < max
            - 'right': min < value <= max

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
            "temperature": {"min": 0, "max": 100, "inclusive": "both"},
            "pressure": {"min": 50, "max": 200, "inclusive": "left"},
        }

        check_value_ranges = CheckValueRanges(
            df=df,
            columns_ranges=columns_ranges,
            default_inclusive="both",
        )

        result_df = check_value_ranges.check()
        ```
    """

    df: PySparkDataFrame

    def __init__(
        self,
        df: PySparkDataFrame,
        columns_ranges: dict,
        default_inclusive: str = "both",
    ) -> None:

        self.df = df
        self.columns_ranges = columns_ranges
        self.default_inclusive = default_inclusive.lower()

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
        Executes the value range checking logic. Identifies and logs any rows where specified
        columns exceed their defined value ranges.

        Returns:
            pyspark.sql.DataFrame:
                Returns the original PySpark DataFrame without changes.
        """
        self._validate_inputs()
        df = self.df

        # For each column and its range, check for out-of-range values
        for column, range_dict in self.columns_ranges.items():
            min_value = range_dict.get("min", None)
            max_value = range_dict.get("max", None)
            inclusive = range_dict.get("inclusive", self.default_inclusive).lower()

            conditions = []

            # Build minimum value condition
            if min_value is not None:
                if inclusive in ["both", "left"]:
                    min_condition = col(column) < min_value
                elif inclusive in ["neither", "right"]:
                    min_condition = col(column) <= min_value
                else:
                    raise ValueError(
                        "Invalid value for 'inclusive' parameter. Must be 'both', 'neither', 'left', or 'right'."
                    )
                conditions.append(min_condition)

            # Build maximum value condition
            if max_value is not None:
                if inclusive in ["both", "right"]:
                    max_condition = col(column) > max_value
                elif inclusive in ["neither", "left"]:
                    max_condition = col(column) >= max_value
                else:
                    raise ValueError(
                        "Invalid value for 'inclusive' parameter. Must be 'both', 'neither', 'left', or 'right'."
                    )
                conditions.append(max_condition)

            if not conditions:
                # Should not happen, as at least 'min' or 'max' must be provided
                continue

            # Combine all conditions with a logical OR
            condition = reduce(or_, conditions)

            # Filter out-of-range rows
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
        # Validate that columns_ranges is a dictionary
        if not isinstance(self.columns_ranges, dict):
            raise TypeError("columns_ranges must be a dictionary.")

        valid_inclusive = ["both", "neither", "left", "right"]

        # Validate default_inclusive parameter
        if self.default_inclusive not in valid_inclusive:
            raise ValueError(
                f"Default inclusive parameter must be one of {valid_inclusive}."
            )

        # Validate that columns exist in DataFrame and parameters are correct
        for column, range_dict in self.columns_ranges.items():
            if column not in self.df.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame.")

            inclusive = range_dict.get("inclusive", self.default_inclusive).lower()
            if inclusive not in valid_inclusive:
                raise ValueError(
                    f"Inclusive parameter for column '{column}' must be one of {valid_inclusive}."
                )

            if "min" not in range_dict and "max" not in range_dict:
                raise ValueError(
                    f"Column '{column}' must have at least 'min' or 'max' specified."
                )
