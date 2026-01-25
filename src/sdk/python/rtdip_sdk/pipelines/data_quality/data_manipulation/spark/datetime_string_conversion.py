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
from pyspark.sql.types import TimestampType
from typing import List, Optional
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


DEFAULT_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss.SSSSSS",
    "yyyy-MM-dd HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy/MM/dd HH:mm:ss",
    "dd-MM-yyyy HH:mm:ss",
]


class DatetimeStringConversion(DataManipulationBaseInterface):
    """
    Converts string-based timestamp columns to datetime with robust format handling.

    This component handles mixed datetime formats commonly found in industrial
    sensor data, including timestamps with and without microseconds, different
    separators, and various date orderings.

    The conversion tries multiple formats sequentially and uses the first
    successful match. Failed conversions result in null values.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.datetime_string_conversion import DatetimeStringConversion
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ('A', '2024-01-02 20:03:46.000'),
        ('B', '2024-01-02 16:00:12.123'),
        ('C', '2024-01-02 11:56:42')
    ], ['sensor_id', 'EventTime'])

    converter = DatetimeStringConversion(
        df,
        column="EventTime",
        output_column="EventTime_DT"
    )
    result_df = converter.filter_data()
    # Result will have a new 'EventTime_DT' column with timestamp values
    ```

    Parameters:
        df (DataFrame): The PySpark DataFrame containing the datetime string column.
        column (str): The name of the column containing datetime strings.
        output_column (str, optional): Name for the output datetime column.
            Defaults to "{column}_DT".
        formats (List[str], optional): List of Spark datetime formats to try.
            Uses Java SimpleDateFormat patterns (e.g., "yyyy-MM-dd HH:mm:ss").
            Defaults to common formats including with/without fractional seconds.
        keep_original (bool, optional): Whether to keep the original string column.
            Defaults to True.
    """

    df: DataFrame
    column: str
    output_column: Optional[str]
    formats: List[str]
    keep_original: bool

    def __init__(
        self,
        df: DataFrame,
        column: str,
        output_column: Optional[str] = None,
        formats: Optional[List[str]] = None,
        keep_original: bool = True,
    ) -> None:
        self.df = df
        self.column = column
        self.output_column = output_column if output_column else f"{column}_DT"
        self.formats = formats if formats is not None else DEFAULT_FORMATS
        self.keep_original = keep_original

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

        if not self.formats:
            raise ValueError("At least one datetime format must be provided.")

        result_df = self.df
        string_col = F.col(self.column).cast("string")

        parse_attempts = [F.to_timestamp(string_col, fmt) for fmt in self.formats]

        result_df = result_df.withColumn(
            self.output_column, F.coalesce(*parse_attempts)
        )

        if not self.keep_original:
            result_df = result_df.drop(self.column)

        return result_df
