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
from pyspark.sql.functions import (
    from_json,
    col,
    explode,
    to_timestamp,
    coalesce,
)
from pyspark.sql.types import ArrayType, StringType

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import OPC_PUBLISHER_AE_SCHEMA


class OPCPublisherOPCAEJsonToPCDMTransformer(TransformerInterface):
    """
    Converts a Spark Dataframe column containing a json string created by OPC Publisher for A&E(Alarm &Events) data to the Process Control Data Model.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import OPCPublisherOPCAEJsonToPCDMTransformer

    opc_publisher_opcae_json_to_pcdm_transformer = OPCPublisherOPCAEJsonToPCDMTransformer(
        data=df,
        souce_column_name="body",
        timestamp_formats=[
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd'T'HH:mm:ssX"
        ],
        filter=None
    )

    result = opc_publisher_opcae_json_to_pcdm_transformer.transform()
    ```

    Parameters:
        data (DataFrame): Dataframe containing the column with Json OPC AE data
        source_column_name (str): Spark Dataframe column containing the OPC Publisher Json OPC AE data
        timestamp_formats (optional list[str]): Specifies the timestamp formats to be used for converting the timestamp string to a Timestamp Type. For more information on formats, refer to this [documentation.](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
        filter (optional str): Enables providing a filter to the data which can be required in certain scenarios. For example, it would be possible to filter on IoT Hub Device Id and Module by providing a filter in SQL format such as `systemProperties.iothub-connection-device-id = "<Device Id>" AND systemProperties.iothub-connection-module-id = "<Module>"`
    """

    data: DataFrame
    source_column_name: str
    timestamp_formats: list
    filter: str

    def __init__(
        self,
        data: DataFrame,
        source_column_name: str,
        timestamp_formats=None,
        filter: str = None,
    ) -> None:  # NOSONAR
        self.data = data
        self.source_column_name = source_column_name
        self.timestamp_formats = timestamp_formats or [
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd'T'HH:mm:ssX",
        ]
        self.filter = filter

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
            DataFrame: A dataframe with the OPC Publisher A&E data converted to the Process Control Data Model
        """

        df = self.data.withColumn(
            self.source_column_name,
            from_json(col(self.source_column_name), ArrayType(StringType())),
        ).withColumn(self.source_column_name, explode(self.source_column_name))

        if self.filter != None:
            df = df.where(self.filter)

        df = df.withColumn(
            "OPCAE", from_json(col(self.source_column_name), OPC_PUBLISHER_AE_SCHEMA)
        )

        df = df.select(
            col("OPCAE.NodeId"),
            col("OPCAE.DisplayName"),
            col("OPCAE.Value.ConditionId.Value").alias("ConditionId"),
            col("OPCAE.Value.AckedState.Value").alias("AckedState"),
            col("OPCAE.Value.AckedState/FalseState.Value").alias(
                "AckedState/FalseState"
            ),
            col("OPCAE.Value.AckedState/Id.Value").alias("AckedState/Id"),
            col("OPCAE.Value.AckedState/TrueState.Value").alias("AckedState/TrueState"),
            col("OPCAE.Value.ActiveState.Value").alias("ActiveState"),
            col("OPCAE.Value.ActiveState/FalseState.Value").alias(
                "ActiveState/FalseState"
            ),
            col("OPCAE.Value.ActiveState/Id.Value").alias("ActiveState/Id"),
            col("OPCAE.Value.ActiveState/TrueState.Value").alias(
                "ActiveState/TrueState"
            ),
            col("OPCAE.Value.EnabledState.Value").alias("EnabledState"),
            col("OPCAE.Value.EnabledState/FalseState.Value").alias(
                "EnabledState/FalseState"
            ),
            col("OPCAE.Value.EnabledState/Id.Value").alias("EnabledState/Id"),
            col("OPCAE.Value.EnabledState/TrueState.Value").alias(
                "EnabledState/TrueState"
            ),
            col("OPCAE.Value.EventId.Value").alias("EventId"),
            col("OPCAE.Value.EventType.Value").alias("EventType"),
            col("OPCAE.Value.HighHighLimit.Value").alias("HighHighLimit"),
            col("OPCAE.Value.HighLimit.Value").alias("HighLimit"),
            col("OPCAE.Value.InputNode.Value").alias("InputNode"),
            col("OPCAE.Value.LowLimit.Value").alias("LowLimit"),
            col("OPCAE.Value.LowLowLimit.Value").alias("LowLowLimit"),
            col("OPCAE.Value.Message.Value").alias("Message"),
            col("OPCAE.Value.Quality.Value").alias("Quality"),
            col("OPCAE.Value.ReceiveTime.Value").alias("ReceiveTime"),
            col("OPCAE.Value.Retain.Value").alias("Retain"),
            col("OPCAE.Value.Severity.Value").alias("Severity"),
            col("OPCAE.Value.SourceName.Value").alias("SourceName"),
            col("OPCAE.Value.SourceNode.Value").alias("SourceNode"),
            col("OPCAE.Value.Time.Value").alias("EventTime"),
        )

        df = df.withColumn(
            "EventTime",
            coalesce(
                *[to_timestamp(col("EventTime"), f) for f in self.timestamp_formats]
            ),
        )

        return df
