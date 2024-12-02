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
from pyspark.sql.functions import desc
from pyspark.sql import DataFrame as PySparkDataFrame

from ..interfaces import DataManipulationBaseInterface
from ...input_validator import InputValidator
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class DuplicateDetection(DataManipulationBaseInterface, InputValidator):
    """
    Cleanses a PySpark DataFrame from duplicates.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.monitoring.spark.data_manipulation.duplicate_detection import DuplicateDetection
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame

    duplicate_detection_monitor = DuplicateDetection(df, primary_key_columns=["TagName", "EventTime"])

    result = duplicate_detection_monitor.filter()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be cleansed.
        primary_key_columns (list): List of column names that serve as primary key for duplicate detection.
    """

    df: PySparkDataFrame
    primary_key_columns: list

    def __init__(self, df: PySparkDataFrame, primary_key_columns: list) -> None:
        if not primary_key_columns or not isinstance(primary_key_columns, list):
            raise ValueError(
                "primary_key_columns must be a non-empty list of column names."
            )
        self.df = df
        self.primary_key_columns = primary_key_columns

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

    def filter(self) -> PySparkDataFrame:
        """
        Returns:
            DataFrame: A cleansed PySpark DataFrame from all duplicates based on primary key columns.
        """
        cleansed_df = self.df.dropDuplicates(self.primary_key_columns)
        return cleansed_df
