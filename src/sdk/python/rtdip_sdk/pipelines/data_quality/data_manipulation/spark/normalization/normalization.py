# Copyright 2024 RTDIP
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
from abc import abstractmethod
from pyspark.sql import DataFrame as PySparkDataFrame
from typing import List
from ....input_validator import InputValidator
from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.interfaces import (
    DataManipulationBaseInterface,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class NormalizationBaseClass(DataManipulationBaseInterface, InputValidator):
    """
    A base class for applying normalization techniques to multiple columns in a PySpark DataFrame.
    This class serves as a framework to support various normalization methods (e.g., Z-Score, Min-Max, and Mean),
    with specific implementations in separate subclasses for each normalization type.

    Subclasses should implement specific normalization and denormalization methods by inheriting from this base class.


    Example
    --------
    ```python
    from src.sdk.python.rtdip_sdk.pipelines.data_wranglers import NormalizationZScore
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame

    normalization = NormalizationZScore(df, column_names=["value_column_1", "value_column_2"], in_place=False)
    normalized_df = normalization.filter()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be normalized.
        column_names (List[str]): List of columns in the DataFrame to be normalized.
        in_place (bool): If true, then result of normalization is stored in the same column.

    Attributes:
    NORMALIZATION_NAME_POSTFIX : str
        Suffix added to the column name if a new column is created for normalized values.

    """

    df: PySparkDataFrame
    column_names: List[str]
    in_place: bool

    reversal_value: List[float]

    # Appended to column name if new column is added
    NORMALIZATION_NAME_POSTFIX: str = "normalization"

    def __init__(
        self, df: PySparkDataFrame, column_names: List[str], in_place: bool = False
    ) -> None:

        for column_name in column_names:
            if not column_name in df.columns:
                raise ValueError("{} not found in the DataFrame.".format(column_name))

        self.df = df
        self.column_names = column_names
        self.in_place = in_place

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

    def filter(self):
        return self.normalize()

    def normalize(self) -> PySparkDataFrame:
        """
        Applies the specified normalization to each column in column_names.

        Returns:
            DataFrame: A PySpark DataFrame with the normalized values.
        """
        normalized_df = self.df
        for column in self.column_names:
            normalized_df = self._normalize_column(normalized_df, column)
        return normalized_df

    def denormalize(self, input_df) -> PySparkDataFrame:
        """
        Denormalizes the input DataFrame. Intended to be used by the denormalization component.

        Parameters:
            input_df (DataFrame): Dataframe containing the current data.
        """
        denormalized_df = input_df
        if not self.in_place:
            for column in self.column_names:
                denormalized_df = denormalized_df.drop(
                    self._get_norm_column_name(column)
                )
        else:
            for column in self.column_names:
                denormalized_df = self._denormalize_column(denormalized_df, column)
        return denormalized_df

    @property
    @abstractmethod
    def NORMALIZED_COLUMN_NAME(self): ...

    @abstractmethod
    def _normalize_column(self, df: PySparkDataFrame, column: str) -> PySparkDataFrame:
        pass

    @abstractmethod
    def _denormalize_column(
        self, df: PySparkDataFrame, column: str
    ) -> PySparkDataFrame:
        pass

    def _get_norm_column_name(self, column_name: str) -> str:
        if not self.in_place:
            return f"{column_name}_{self.NORMALIZED_COLUMN_NAME}_{self.NORMALIZATION_NAME_POSTFIX}"
        else:
            return column_name
