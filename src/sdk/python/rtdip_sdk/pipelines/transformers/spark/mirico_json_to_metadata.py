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
import logging
from pyspark.sql.functions import (
    from_json,
    col,
    lit,
    concat_ws,
    upper,
    expr,
)
from ...._sdk_utils.compare_versions import (
    _package_version_meets_minimum,
)
from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import MIRICO_METADATA_SCHEMA


class MiricoJsonToMetadataTransformer(TransformerInterface):
    """
    Converts a Spark Dataframe column containing a json string created from Mirico to the Metadata Model.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import MiricoJsonToMetadataTransformer

    mirico_json_to_metadata_transformer = MiricoJsonToMetadataTransformer(
        data=df
        source_column_name="body"
    )

    result = mirico_json_to_metadata_transformer.transform()
    ```

    Parameters:
        data (DataFrame): Dataframe containing the column with Mirico data
        source_column_name (str): Spark Dataframe column containing the Json Mirico data
    """

    data: DataFrame
    source_column_name: str

    def __init__(self, data: DataFrame, source_column_name: str) -> None:
        _package_version_meets_minimum("pyspark", "3.4.0")
        self.data = data
        self.source_column_name = source_column_name

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
            DataFrame: A dataframe with the specified column converted to Metadata model
        """

        df = self.data.select(
            from_json(self.source_column_name, MIRICO_METADATA_SCHEMA).alias("body"),
        )

        tag_name_expr = concat_ws(
            "_",
            *[
                upper(col("body.siteName")),
                upper(col("body.retroName")),
                upper(col("body.gasType")),
            ]
        )

        df = df.select(
            tag_name_expr.alias("TagName"),
            lit("").alias("Description"),
            lit("").alias("UoM"),
            expr(
                """struct(
                body.retroAltitude,
                body.retroLongitude,
                body.retroLatitude,
                body.sensorAltitude,
                body.sensorLongitude,
                body.sensorLatitude)"""
            ).alias("Properties"),
        ).dropDuplicates(["TagName"])

        return df.select("TagName", "Description", "UoM", "Properties")
