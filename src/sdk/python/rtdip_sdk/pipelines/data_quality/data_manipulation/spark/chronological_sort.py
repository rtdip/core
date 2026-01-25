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
from typing import List, Optional
from ..interfaces import DataManipulationBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class ChronologicalSort(DataManipulationBaseInterface):
    """
    Sorts a DataFrame chronologically by a datetime column.

    This component is essential for time series preprocessing to ensure
    data is in the correct temporal order before applying operations
    like lag features, rolling statistics, or time-based splits.

    Note: In distributed Spark environments, sorting is a global operation
    that requires shuffling data across partitions. For very large datasets,
    consider whether global ordering is necessary or if partition-level
    ordering would suffice.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.data_quality.data_manipulation.spark.chronological_sort import ChronologicalSort
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
        ('A', '2024-01-03', 30),
        ('B', '2024-01-01', 10),
        ('C', '2024-01-02', 20)
    ], ['sensor_id', 'timestamp', 'value'])

    sorter = ChronologicalSort(df, datetime_column="timestamp")
    result_df = sorter.filter_data()
    # Result will be sorted: 2024-01-01, 2024-01-02, 2024-01-03
    ```

    Parameters:
        df (DataFrame): The PySpark DataFrame to sort.
        datetime_column (str): The name of the datetime column to sort by.
        ascending (bool, optional): Sort in ascending order (oldest first).
            Defaults to True.
        group_columns (List[str], optional): Columns to group by before sorting.
            If provided, sorting is done within each group. Defaults to None.
        nulls_last (bool, optional): Whether to place null values at the end.
            Defaults to True.
    """

    df: DataFrame
    datetime_column: str
    ascending: bool
    group_columns: Optional[List[str]]
    nulls_last: bool

    def __init__(
        self,
        df: DataFrame,
        datetime_column: str,
        ascending: bool = True,
        group_columns: Optional[List[str]] = None,
        nulls_last: bool = True,
    ) -> None:
        self.df = df
        self.datetime_column = datetime_column
        self.ascending = ascending
        self.group_columns = group_columns
        self.nulls_last = nulls_last

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

        if self.datetime_column not in self.df.columns:
            raise ValueError(
                f"Column '{self.datetime_column}' does not exist in the DataFrame."
            )

        if self.group_columns:
            for col in self.group_columns:
                if col not in self.df.columns:
                    raise ValueError(
                        f"Group column '{col}' does not exist in the DataFrame."
                    )

        if self.ascending:
            if self.nulls_last:
                datetime_sort = F.col(self.datetime_column).asc_nulls_last()
            else:
                datetime_sort = F.col(self.datetime_column).asc_nulls_first()
        else:
            if self.nulls_last:
                datetime_sort = F.col(self.datetime_column).desc_nulls_last()
            else:
                datetime_sort = F.col(self.datetime_column).desc_nulls_first()

        if self.group_columns:
            sort_expressions = [F.col(c).asc() for c in self.group_columns]
            sort_expressions.append(datetime_sort)
            result_df = self.df.orderBy(*sort_expressions)
        else:
            result_df = self.df.orderBy(datetime_sort)

        return result_df
