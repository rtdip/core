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
from pyspark.sql.types import DoubleType, StringType
from typing import Union
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class MixedTypeSeparation(DataManipulationBaseInterface):
    """
    Separates textual values from a mixed-type string column.

    This is useful when a column contains both numeric values and textual
    status indicators (e.g., "Bad", "Error", "N/A") stored as strings.
    The component extracts non-numeric strings into a separate column and
    converts numeric strings to actual numeric values, replacing non-numeric
    entries with a placeholder value.

    Note: The input column must be of StringType. In Spark, columns are strongly
    typed, so mixed numeric/string data is typically stored as strings.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.mixed_type_separation import MixedTypeSeparation
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ('A', '3.14'),
        ('B', 'Bad'),
        ('C', '100'),
        ('D', 'Error')
    ], ['sensor_id', 'value'])

    separator = MixedTypeSeparation(
        df,
        column="value",
        placeholder=-1.0,
        string_fill="NaN"
    )
    result_df = separator.filter_data()
    # Result:
    #   sensor_id  value  value_str
    #   A          3.14   NaN
    #   B          -1.0   Bad
    #   C          100.0  NaN
    #   D          -1.0   Error
    ```

    Parameters:
        df (DataFrame): The PySpark DataFrame containing the mixed-type string column.
        column (str): The name of the column to separate (must be StringType).
        placeholder (Union[int, float], optional): Value to replace non-numeric entries
            in the numeric column. Defaults to -1.0.
        string_fill (str, optional): Value to fill in the string column for numeric entries.
            Defaults to "NaN".
        suffix (str, optional): Suffix for the new string column name.
            Defaults to "_str".
    """

    df: DataFrame
    column: str
    placeholder: Union[int, float]
    string_fill: str
    suffix: str

    def __init__(
        self,
        df: DataFrame,
        column: str,
        placeholder: Union[int, float] = -1.0,
        string_fill: str = "NaN",
        suffix: str = "_str",
    ) -> None:
        self.df = df
        self.column = column
        self.placeholder = placeholder
        self.string_fill = string_fill
        self.suffix = suffix

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

    def filter_data(self) -> DataFrame:
        if self.df is None:
            raise ValueError("The DataFrame is None.")

        if self.column not in self.df.columns:
            raise ValueError(f"Column '{self.column}' does not exist in the DataFrame.")

        result_df = self.df
        string_col_name = f"{self.column}{self.suffix}"

        result_df = result_df.withColumn(
            "_temp_string_col", F.col(self.column).cast(StringType())
        )

        result_df = result_df.withColumn(
            "_temp_numeric_col", F.col("_temp_string_col").cast(DoubleType())
        )

        is_non_numeric = (
            F.col("_temp_string_col").isNotNull() & F.col("_temp_numeric_col").isNull()
        )

        result_df = result_df.withColumn(
            string_col_name,
            F.when(is_non_numeric, F.col("_temp_string_col")).otherwise(
                F.lit(self.string_fill)
            ),
        )

        result_df = result_df.withColumn(
            self.column,
            F.when(is_non_numeric, F.lit(self.placeholder)).otherwise(
                F.col("_temp_numeric_col")
            ),
        )

        result_df = result_df.drop("_temp_string_col", "_temp_numeric_col")

        return result_df
