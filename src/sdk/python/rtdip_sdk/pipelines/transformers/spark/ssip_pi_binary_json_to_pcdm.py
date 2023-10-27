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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, get_json_object, element_at, when

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ...sources.spark.delta import SparkDeltaSource


class SSIPPIJsonStreamToPCDMTransformer(TransformerInterface):
    """
    Converts a Spark DataFrame containing Binary JSON data and related Properties to the Process Control Data Model

    For more information about the SSIP PI Streaming Connector, please see [here.](https://bakerhughesc3.ai/oai-solution/shell-sensor-intelligence-platform/)

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import SSIPPIJsonStreamToPCDMTransformer
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    ssip_pi_json_stream_to_pcdm_transformer = SSIPPIJsonStreamToPCDMTransformer(
        spark=spark,
        data=df,
        source_column_name="body",
        properties_column_name="",
        metadata_delta_table=None
    )

    result = ssip_pi_json_stream_to_pcdm_transformer.transform()
    ```

    Parameters:
        spark (SparkSession): Spark Session
        data (DataFrame): DataFrame containing the path and binaryFile data
        source_column_name (str): Spark Dataframe column containing the Binary json data
        properties_column_name (str): Spark Dataframe struct typed column containing an element with the PointType
        metadata_delta_table (optional, str): Name of a metadata table that can be used for PointType mappings
    """

    spark: SparkSession
    data: DataFrame
    source_column_name: str
    properties_column_name: str
    metadata_delta_table: str

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
        source_column_name: str,
        properties_column_name: str,
        metadata_delta_table: str = None,
    ) -> None:
        self.spark = spark
        self.data = data
        self.source_column_name = source_column_name
        self.properties_column_name = properties_column_name
        self.metadata_delta_table = metadata_delta_table

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
            DataFrame: A dataframe with the provided Binary data converted to PCDM
        """
        df = (
            self.data.withColumn(
                self.source_column_name, col(self.source_column_name).cast("string")
            )
            .withColumn(
                "EventDate",
                get_json_object(col(self.source_column_name), "$.EventTime").cast(
                    "date"
                ),
            )
            .withColumn(
                "TagName",
                get_json_object(col(self.source_column_name), "$.TagName").cast(
                    "string"
                ),
            )
            .withColumn(
                "EventTime",
                get_json_object(col(self.source_column_name), "$.EventTime").cast(
                    "timestamp"
                ),
            )
            .withColumn(
                "Status",
                get_json_object(col(self.source_column_name), "$.Quality").cast(
                    "string"
                ),
            )
            .withColumn(
                "Value",
                get_json_object(col(self.source_column_name), "$.Value").cast("string"),
            )
            .withColumn(
                "PointType", element_at(col(self.properties_column_name), "PointType")
            )
            .withColumn(
                "Action",
                element_at(col(self.properties_column_name), "Action").cast("string"),
            )
        )

        if self.metadata_delta_table != None:
            metadata_df = SparkDeltaSource(
                self.spark, {}, self.metadata_delta_table
            ).read_batch()
            metadata_df = metadata_df.select(
                "TagName", col("PointType").alias("MetadataPointType")
            )
            df = df.join(metadata_df, (df.TagName == metadata_df.TagName), "left")
            df = df.withColumn(
                "PointType",
                (when(col("PointType").isNull(), col("MetadataPointType"))).otherwise(
                    col("PointType")
                ),
            )

        return (
            df.withColumn(
                "ValueType",
                (
                    when(col("PointType") == "Digital", "string")
                    .when(col("PointType") == "String", "string")
                    .when(col("PointType") == "Float16", "float")
                    .when(col("PointType") == "Float32", "float")
                    .when(col("PointType") == "Float64", "float")
                    .when(col("PointType") == "Int16", "integer")
                    .when(col("PointType") == "Int32", "integer")
                    .otherwise("string")
                ),
            )
            .selectExpr(
                "*",
                "CASE WHEN ValueType = 'integer' THEN try_cast(Value as integer) END as Value_Integer",
                "CASE WHEN ValueType = 'float' THEN try_cast(Value as float) END as Value_Float",
            )
            .withColumn(
                "ValueType",
                when(
                    (col("Value_Integer").isNull()) & (col("ValueType") == "integer"),
                    "string",
                )
                .when(
                    (col("Value_Float").isNull()) & (col("ValueType") == "float"),
                    "string",
                )
                .otherwise(col("ValueType")),
            )
            .withColumn(
                "ChangeType",
                (
                    when(col("Action") == "Insert", "insert")
                    .when(col("Action") == "Add", "insert")
                    .when(col("Action") == "Delete", "delete")
                    .when(col("Action") == "Update", "update")
                    .when(col("Action") == "Refresh", "update")
                ),
            )
            .select(
                col("EventDate"),
                col("TagName"),
                col("EventTime"),
                col("Status"),
                col("Value"),
                col("ValueType"),
                col("ChangeType"),
            )
        )
