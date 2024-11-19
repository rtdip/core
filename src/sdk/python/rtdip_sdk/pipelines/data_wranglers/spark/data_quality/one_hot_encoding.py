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

from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.sql import functions as F
from ...interfaces import WranglerBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class OneHotEncoding(WranglerBaseInterface):
    """
    Performs One-Hot Encoding on a specified column of a PySpark DataFrame.

    Example
    --------
    ```python
    from src.sdk.python.rtdip_sdk.pipelines.data_wranglers.spark.data_quality.one_hot_encoding import OneHotEncoding
    from pyspark.sql import SparkSession
    

    spark = ... # SparkSession
    df = ... # Get a PySpark DataFrame

    one_hot_encoder = OneHotEncoding(df, "column_name", ["list_of_distinct_values"])
    result_df = one_hot_encoder.encode()
    result_df.show()
    ```

    Parameters:
        df (DataFrame): The PySpark DataFrame to apply encoding on.
        column (str): The name of the column to apply the encoding to.
        values (list, optional): A list of distinct values to encode. If not provided,
                                 the distinct values from the data will be used.
    """

    df: PySparkDataFrame
    column: str
    values: list

    def __init__(self, df: PySparkDataFrame, column: str, values: list = None) -> None:
        self.df = df
        self.column = column
        self.values = values

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
        if not self.values:
            self.values = [row[self.column] for row in self.df.select(self.column).distinct().collect()]

        for value in self.values:
            self.df = self.df.withColumn(
                f"{self.column}_{value}",
                F.when(F.col(self.column) == value, 1).otherwise(0)
            )
        return self.df
