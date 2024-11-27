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

from pyspark.ml.feature import VectorAssembler
from pyspark.sql import DataFrame
from ...._pipeline_utils.models import Libraries, SystemType
from ...interfaces import TransformerInterface


class ColumnsToVector(TransformerInterface):
    """
    Converts columns containing numbers to a column containing a vector.

    Parameters:
        df (DataFrame): PySpark DataFrame
        input_cols (list[str]): List of columns to convert to a vector.
        output_col (str): Name of the output column where the vector will be stored.
        override_col (bool): If True, the output column can override an existing column.
    """

    def __init__(
        self,
        df: DataFrame,
        input_cols: list[str],
        output_col: str,
        override_col: bool = False,
    ) -> None:
        self.input_cols = input_cols
        self.output_col = output_col
        self.override_col = override_col
        self.df = df

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

    def pre_transform_validation(self):
        if self.output_col in self.df.columns and not self.override_col:
            return False
        return True

    def post_transform_validation(self):
        return True

    def transform(self):
        if not self.pre_transform_validation():
            raise ValueError(
                f"Output column {self.output_col} already exists and override_col is set to False."
            )

        temp_col = (
            f"{self.output_col}_temp" if self.output_col in self.df.columns else None
        )
        transformed_df = VectorAssembler(
            inputCols=self.input_cols, outputCol=(temp_col or self.output_col)
        ).transform(self.df)

        if temp_col:
            return transformed_df.drop(self.output_col).withColumnRenamed(
                temp_col, self.output_col
            )
        return transformed_df
