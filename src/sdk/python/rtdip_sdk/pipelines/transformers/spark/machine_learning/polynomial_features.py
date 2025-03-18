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

import pyspark.ml as ml
from pyspark.sql import DataFrame

from ...._pipeline_utils.models import Libraries, SystemType
from ...interfaces import TransformerInterface


class PolynomialFeatures(TransformerInterface):
    """
    This transformer takes a vector column and generates polynomial combinations of the input features
    up to the specified degree. For example, if the input vector is [a, b] and degree=2,
    the output features will be [a, b, a^2, ab, b^2].

    Parameters:
        df (DataFrame): PySpark DataFrame
        input_col (str): Name of the input column in the DataFrame that contains the feature vectors
        output_col (str):
        poly_degree (int): The degree of the polynomial features to generate
        override_col (bool): If True, the output column can override an existing column.
    """

    def __init__(
        self,
        df: DataFrame,
        input_col: str,
        output_col: str,
        poly_degree: int,
        override_col: bool = False,
    ):
        self.df = df
        self.input_col = input_col
        self.output_col = output_col
        self.poly_degree = poly_degree
        self.override_col = override_col

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
        if not (self.input_col in self.df.columns):
            raise ValueError(
                f"Input column '{self.input_col}' does not exist in the DataFrame."
            )
        if self.output_col in self.df.columns and not self.override_col:
            raise ValueError(
                f"Output column '{self.output_col}' already exists in the DataFrame and override_col is set to False."
            )
        if not isinstance(self.df.schema[self.input_col].dataType, ml.linalg.VectorUDT):
            raise ValueError(
                f"Input column '{self.input_col}' is not of type VectorUDT."
            )
        return True

    def post_transform_validation(self):
        if self.output_col not in self.df.columns:
            raise ValueError(
                f"Output column '{self.output_col}' does not exist in the transformed DataFrame."
            )
        return True

    def transform(self):

        self.pre_transform_validation()

        temp_col = (
            f"{self.output_col}_temp" if self.output_col in self.df.columns else None
        )
        transformed_df = ml.feature.PolynomialExpansion(
            degree=self.poly_degree,
            inputCol=self.input_col,
            outputCol=(temp_col or self.output_col),
        ).transform(self.df)

        if temp_col:
            return transformed_df.drop(self.output_col).withColumnRenamed(
                temp_col, self.output_col
            )

        self.df = transformed_df
        self.post_transform_validation()

        return transformed_df
