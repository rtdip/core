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

from pyspark.sql import DataFrame

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType


class BinaryToStringTransformer(TransformerInterface):
    """
    Converts a dataframe body column from a binary to a string.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import BinaryToStringTransformer

    binary_to_string_transformer = BinaryToStringTransformer(
        data=df,
        souce_column_name="body",
        target_column_name="body"
    )

    result = binary_to_string_transformer.transform()
    ```

    Parameters:
        data (DataFrame): Dataframe to be transformed
        source_column_name (str): Spark Dataframe column containing the Binary data
        target_column_name (str): Spark Dataframe column name to be used for the String data
    """

    data: DataFrame
    source_column_name: str
    target_column_name: str

    def __init__(
        self, data: DataFrame, source_column_name: str, target_column_name: str
    ) -> None:
        self.data = data
        self.source_column_name = source_column_name
        self.target_column_name = target_column_name

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
        return True

    def post_transform_validation(self):
        return True

    def transform(self) -> DataFrame:
        """
        Returns:
            DataFrame: A dataframe with the body column converted to string.
        """
        return self.data.withColumn(
            self.target_column_name, self.data[self.source_column_name].cast("string")
        )
