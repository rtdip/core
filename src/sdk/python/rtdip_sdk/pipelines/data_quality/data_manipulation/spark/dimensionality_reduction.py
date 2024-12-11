# Copyright 2024 Project Team
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
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

from ..interfaces import DataManipulationBaseInterface
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class DimensionalityReduction(DataManipulationBaseInterface):
    """
    Detects and combines columns based on correlation or exact duplicates.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.monitoring.spark.data_manipulation.column_correlation import ColumnCorrelationDetection
    from pyspark.sql import SparkSession

    column_correlation_monitor = ColumnCorrelationDetection(
        df,
        columns_to_check=['column1', 'column2'],
        threshold=0.95,
        combination_method='mean'
    )

    result = column_correlation_monitor.process()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be analyzed and transformed.
        columns (list): List of column names to check for correlation. Only two columns are supported.
        threshold (float, optional): Correlation threshold for column combination [0-1]. If the absolute value of the correlation is equal or bigger, than the columns are combined. Defaults to 0.9.
        combination_method (str, optional): Method to combine correlated columns.
            Supported methods:
            - 'mean': Average the values of both columns and write the result to the first column
              (New value = (column1 + column2) / 2)
            - 'sum': Sum the values of both columns and write the result to the first column
              (New value = column1 + column2)
            - 'first': Keep the first column, drop the second column
            - 'second': Keep the second column, drop the first column
            - 'delete': Remove both columns entirely from the DataFrame
            Defaults to 'mean'.
    """

    df: PySparkDataFrame
    columns_to_check: list
    threshold: float
    combination_method: str

    def __init__(
        self,
        df: PySparkDataFrame,
        columns: list,
        threshold: float = 0.9,
        combination_method: str = "mean",
    ) -> None:
        # Validate inputs
        if not columns or not isinstance(columns, list):
            raise ValueError("columns must be a non-empty list of column names.")
        if len(columns) != 2:
            raise ValueError(
                "columns must contain exactly two columns for correlation."
            )

        if not 0 <= threshold <= 1:
            raise ValueError("Threshold must be between 0 and 1.")

        valid_methods = ["mean", "sum", "first", "second", "delete"]
        if combination_method not in valid_methods:
            raise ValueError(f"combination_method must be one of {valid_methods}")

        self.df = df
        self.columns_to_check = columns
        self.threshold = threshold
        self.combination_method = combination_method

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

    def _calculate_correlation(self) -> float:
        """
        Calculate correlation between specified columns.

        Returns:
            dict: Correlation matrix between columns
        """
        assembler = VectorAssembler(
            inputCols=self.columns_to_check, outputCol="features"
        )
        vector_df = assembler.transform(self.df)

        correlation_matrix = Correlation.corr(
            vector_df, "features", method="pearson"
        ).collect()[0][0]

        # Correlation between first and second column
        return correlation_matrix.toArray()[0][1]

    def filter(self) -> PySparkDataFrame:
        """
        Process DataFrame by detecting and combining correlated columns.

        Returns:
            DataFrame: Transformed PySpark DataFrame
        """
        correlation = self._calculate_correlation()

        # If correlation is below threshold, return original DataFrame
        if correlation < self.threshold:
            return self.df

        col1, col2 = self.columns_to_check
        if self.combination_method == "mean":
            return self.df.withColumn(col1, (col(col1) + col(col2)) / 2).drop(col2)
        elif self.combination_method == "sum":
            return self.df.withColumn(col1, col(col1) + col(col2)).drop(col2)
        elif self.combination_method == "first":
            return self.df.drop(col2)
        elif self.combination_method == "second":
            return self.df.drop(col2)
        elif self.combination_method == "delete":
            return self.df.drop(col1).drop(col2)
        else:
            return self.df
