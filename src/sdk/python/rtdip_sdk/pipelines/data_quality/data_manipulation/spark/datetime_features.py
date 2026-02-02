# Copyright 2026 RTDIP
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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Optional
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


# Available datetime features that can be extracted
AVAILABLE_FEATURES = [
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "weekday",
    "day_name",
    "quarter",
    "week",
    "day_of_year",
    "is_weekend",
    "is_month_start",
    "is_month_end",
    "is_quarter_start",
    "is_quarter_end",
    "is_year_start",
    "is_year_end",
]


class DatetimeFeatures(DataManipulationBaseInterface):
    """
    Extracts datetime/time-based features from a datetime column.

    This is useful for time series forecasting where temporal patterns
    (seasonality, day-of-week effects, etc.) are important predictors.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.datetime_features import DatetimeFeatures
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ('2024-01-01', 1),
        ('2024-01-02', 2),
        ('2024-01-03', 3)
    ], ['timestamp', 'value'])

    # Extract specific features
    extractor = DatetimeFeatures(
        df,
        datetime_column="timestamp",
        features=["year", "month", "weekday", "is_weekend"]
    )
    result_df = extractor.filter_data()
    # Result will have columns: timestamp, value, year, month, weekday, is_weekend
    ```

    Available features:
        - year: Year (e.g., 2024)
        - month: Month (1-12)
        - day: Day of month (1-31)
        - hour: Hour (0-23)
        - minute: Minute (0-59)
        - second: Second (0-59)
        - weekday: Day of week (0=Monday, 6=Sunday)
        - day_name: Name of day ("Monday", "Tuesday", etc.)
        - quarter: Quarter (1-4)
        - week: Week of year (1-52)
        - day_of_year: Day of year (1-366)
        - is_weekend: Boolean, True if Saturday or Sunday
        - is_month_start: Boolean, True if first day of month
        - is_month_end: Boolean, True if last day of month
        - is_quarter_start: Boolean, True if first day of quarter
        - is_quarter_end: Boolean, True if last day of quarter
        - is_year_start: Boolean, True if first day of year
        - is_year_end: Boolean, True if last day of year

    Parameters:
        df (DataFrame): The PySpark DataFrame containing the datetime column.
        datetime_column (str): The name of the column containing datetime values.
        features (List[str], optional): List of features to extract.
            Defaults to ["year", "month", "day", "weekday"].
        prefix (str, optional): Prefix to add to new column names. Defaults to None.
    """

    df: DataFrame
    datetime_column: str
    features: List[str]
    prefix: Optional[str]

    def __init__(
        self,
        df: DataFrame,
        datetime_column: str,
        features: Optional[List[str]] = None,
        prefix: Optional[str] = None,
    ) -> None:
        self.df = df
        self.datetime_column = datetime_column
        self.features = (
            features if features is not None else ["year", "month", "day", "weekday"]
        )
        self.prefix = prefix

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

    def _get_feature_expression(self, feature: str, dt_col):
        """
        Get the PySpark expression for a specific datetime feature.

        Args:
            feature: Name of the feature to extract
            dt_col: The timestamp column expression

        Returns:
            PySpark column expression for the feature
        """
        feature_map = {
            "year": F.year(dt_col),
            "month": F.month(dt_col),
            "day": F.dayofmonth(dt_col),
            "hour": F.hour(dt_col),
            "minute": F.minute(dt_col),
            "second": F.second(dt_col),
            "quarter": F.quarter(dt_col),
            "week": F.weekofyear(dt_col),
            "day_of_year": F.dayofyear(dt_col),
        }

        if feature in feature_map:
            return feature_map[feature]
        elif feature == "weekday":
            return (F.dayofweek(dt_col) + 5) % 7
        elif feature == "day_name":
            return self._get_day_name_expression(dt_col)
        elif feature == "is_weekend":
            return F.dayofweek(dt_col).isin([1, 7])
        elif feature == "is_month_start":
            return F.dayofmonth(dt_col) == 1
        elif feature == "is_month_end":
            return F.month(dt_col) != F.month(F.date_add(dt_col, 1))
        elif feature == "is_quarter_start":
            return (F.month(dt_col).isin([1, 4, 7, 10])) & (F.dayofmonth(dt_col) == 1)
        elif feature == "is_quarter_end":
            return (F.month(dt_col).isin([3, 6, 9, 12])) & (
                F.month(dt_col) != F.month(F.date_add(dt_col, 1))
            )
        elif feature == "is_year_start":
            return (F.month(dt_col) == 1) & (F.dayofmonth(dt_col) == 1)
        elif feature == "is_year_end":
            return (F.month(dt_col) == 12) & (F.dayofmonth(dt_col) == 31)

    def _get_day_name_expression(self, dt_col):
        """Create day name mapping expression."""
        day_names = {
            1: "Sunday",
            2: "Monday",
            3: "Tuesday",
            4: "Wednesday",
            5: "Thursday",
            6: "Friday",
            7: "Saturday",
        }
        mapping_expr = F.create_map(
            [F.lit(x) for pair in day_names.items() for x in pair]
        )
        return mapping_expr[F.dayofweek(dt_col)]

    def _validate_inputs(self):
        """Validate DataFrame and column existence."""
        if self.df is None:
            raise ValueError("The DataFrame is None.")

        if self.datetime_column not in self.df.columns:
            raise ValueError(
                f"Column '{self.datetime_column}' does not exist in the DataFrame."
            )

        invalid_features = set(self.features) - set(AVAILABLE_FEATURES)
        if invalid_features:
            raise ValueError(
                f"Invalid features: {invalid_features}. "
                f"Available features: {AVAILABLE_FEATURES}"
            )

    def filter_data(self) -> DataFrame:
        """
        Extracts the specified datetime features from the datetime column.

        Returns:
            DataFrame: DataFrame with added datetime feature columns.

        Raises:
            ValueError: If the DataFrame is empty, column doesn't exist,
                       or invalid features are requested.
        """
        self._validate_inputs()

        result_df = self.df
        dt_col = F.to_timestamp(F.col(self.datetime_column))

        for feature in self.features:
            col_name = f"{self.prefix}_{feature}" if self.prefix else feature
            feature_expr = self._get_feature_expression(feature, dt_col)
            result_df = result_df.withColumn(col_name, feature_expr)

        return result_df
