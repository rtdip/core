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

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from typing import Optional, Union, List
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


# Constant to convert MAD to standard deviation equivalent for normal distributions
MAD_TO_STD_CONSTANT = 1.4826


class MADOutlierDetection(DataManipulationBaseInterface):
    """
    Detects and handles outliers using Median Absolute Deviation (MAD).

    MAD is a robust measure of variability that is less sensitive to extreme
    outliers compared to standard deviation. This makes it ideal for detecting
    outliers in sensor data that may contain extreme values or data corruption.

    The MAD is defined as: MAD = median(|X - median(X)|)

    Outliers are identified as values that fall outside:
    median ± (n_sigma * MAD * 1.4826)

    Where 1.4826 is a constant that makes MAD comparable to standard deviation
    for normally distributed data.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.mad_outlier_detection import MADOutlierDetection
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ('A', 10.0),
        ('B', 12.0),
        ('C', 11.0),
        ('D', 1000000.0),  # Outlier
        ('E', 9.0)
    ], ['sensor_id', 'value'])

    detector = MADOutlierDetection(
        df,
        column="value",
        n_sigma=3.0,
        action="replace",
        replacement_value=-1.0
    )
    result_df = detector.filter_data()
    # Result will have the outlier replaced with -1.0
    ```

    Parameters:
        df (DataFrame): The PySpark DataFrame containing the value column.
        column (str): The name of the column to check for outliers.
        n_sigma (float, optional): Number of MAD-based standard deviations for
            outlier threshold. Defaults to 3.0.
        action (str, optional): Action to take on outliers. Options:
            - "flag": Add a boolean column indicating outliers
            - "replace": Replace outliers with replacement_value
            - "remove": Remove rows containing outliers
            Defaults to "flag".
        replacement_value (Union[int, float], optional): Value to use when
            action="replace". Defaults to None (uses null).
        exclude_values (List[Union[int, float]], optional): Values to exclude from
            outlier detection (e.g., error codes like -1). Defaults to None.
        outlier_column (str, optional): Name for the outlier flag column when
            action="flag". Defaults to "{column}_is_outlier".
    """

    df: DataFrame
    column: str
    n_sigma: float
    action: str
    replacement_value: Optional[Union[int, float]]
    exclude_values: Optional[List[Union[int, float]]]
    outlier_column: Optional[str]

    def __init__(
        self,
        df: DataFrame,
        column: str,
        n_sigma: float = 3.0,
        action: str = "flag",
        replacement_value: Optional[Union[int, float]] = None,
        exclude_values: Optional[List[Union[int, float]]] = None,
        outlier_column: Optional[str] = None,
    ) -> None:
        self.df = df
        self.column = column
        self.n_sigma = n_sigma
        self.action = action
        self.replacement_value = replacement_value
        self.exclude_values = exclude_values
        self.outlier_column = (
            outlier_column if outlier_column else f"{column}_is_outlier"
        )

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def _compute_mad_bounds(self, df: DataFrame) -> tuple:
        median = df.approxQuantile(self.column, [0.5], 0.0)[0]

        if median is None:
            return None, None

        df_with_dev = df.withColumn(
            "_abs_deviation", F.abs(F.col(self.column) - F.lit(median))
        )

        mad = df_with_dev.approxQuantile("_abs_deviation", [0.5], 0.0)[0]

        if mad is None:
            return None, None

        std_equivalent = mad * MAD_TO_STD_CONSTANT

        lower_bound = median - (self.n_sigma * std_equivalent)
        upper_bound = median + (self.n_sigma * std_equivalent)

        return lower_bound, upper_bound

    def filter_data(self) -> DataFrame:
        if self.df is None:
            raise ValueError("The DataFrame is None.")

        if self.column not in self.df.columns:
            raise ValueError(f"Column '{self.column}' does not exist in the DataFrame.")

        valid_actions = ["flag", "replace", "remove"]
        if self.action not in valid_actions:
            raise ValueError(
                f"Invalid action '{self.action}'. Must be one of {valid_actions}."
            )

        if self.n_sigma <= 0:
            raise ValueError(f"n_sigma must be positive, got {self.n_sigma}.")

        result_df = self.df

        include_condition = F.col(self.column).isNotNull()

        if self.exclude_values is not None and len(self.exclude_values) > 0:
            include_condition = include_condition & ~F.col(self.column).isin(
                self.exclude_values
            )

        valid_df = result_df.filter(include_condition)

        if valid_df.count() == 0:
            if self.action == "flag":
                result_df = result_df.withColumn(self.outlier_column, F.lit(False))
            return result_df

        lower_bound, upper_bound = self._compute_mad_bounds(valid_df)

        if lower_bound is None or upper_bound is None:
            if self.action == "flag":
                result_df = result_df.withColumn(self.outlier_column, F.lit(False))
            return result_df

        is_outlier = include_condition & (
            (F.col(self.column) < F.lit(lower_bound))
            | (F.col(self.column) > F.lit(upper_bound))
        )

        if self.action == "flag":
            result_df = result_df.withColumn(self.outlier_column, is_outlier)

        elif self.action == "replace":
            replacement = (
                F.lit(self.replacement_value)
                if self.replacement_value is not None
                else F.lit(None).cast(DoubleType())
            )
            result_df = result_df.withColumn(
                self.column,
                F.when(is_outlier, replacement).otherwise(F.col(self.column)),
            )

        elif self.action == "remove":
            result_df = result_df.filter(~is_outlier)

        return result_df
