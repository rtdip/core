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

from pyspark.sql import DataFrame, Window, SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import (
    to_json,
    col,
    struct,
    lit,
    array,
    floor,
    row_number,
    collect_list,
    expr,
    udf,
    sha2,
    when,
)
from datetime import datetime
import pytz
import gzip
import base64

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType


class PCDMToHoneywellAPMTransformer(TransformerInterface):
    """
    Converts a Spark Dataframe in PCDM format to Honeywell APM format.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import PCDMToHoneywellAPMTransformer

    pcdm_to_honeywell_apm_transformer = PCDMToHoneywellAPMTransformer(
        data=df,
        quality="Good",
        history_samples_per_message=1,
        compress_payload=True
    )

    result = pcdm_to_honeywell_apm_transformer.transform()
    ```

    Parameters:
        data (Dataframe): Spark Dataframe in PCDM format
        quality (str): Value for quality inside HistorySamples
        history_samples_per_message (int): The number of HistorySamples for each row in the DataFrame (Batch Only)
        compress_payload (bool): If True compresses CloudPlatformEvent with gzip compression
    """

    data: DataFrame
    quality: str
    history_samples_per_message: int
    compress_payload: bool

    def __init__(
        self,
        data: DataFrame,
        quality: str = "Good",
        history_samples_per_message: int = 1,
        compress_payload: bool = True,
    ) -> None:
        self.data = data
        self.quality = quality
        self.history_samples_per_message = history_samples_per_message
        self.compress_payload = compress_payload

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
            DataFrame: A dataframe with with rows in Honeywell APM format
        """

        @udf("string")
        def _compress_payload(data):
            compressed_data = gzip.compress(data.encode("utf-8"))
            encoded_data = base64.b64encode(compressed_data).decode("utf-8")
            return encoded_data

        if self.data.isStreaming == False and self.history_samples_per_message > 1:
            w = Window.partitionBy("TagName").orderBy("TagName")
            cleaned_pcdm_df = (
                self.data.withColumn(
                    "index",
                    floor(
                        (row_number().over(w) - 0.01) / self.history_samples_per_message
                    ),
                )
                .withColumn(
                    "HistorySamples",
                    struct(
                        col("TagName").alias("ItemName"),
                        lit(self.quality).alias("Quality"),
                        col("EventTime").alias("Time"),
                        col("Value").alias("Value"),
                    ).alias("HistorySamples"),
                )
                .groupBy("TagName", "index")
                .agg(collect_list("HistorySamples").alias("HistorySamples"))
                .withColumn("guid", sha2(col("TagName"), 256).cast("string"))
                .withColumn(
                    "value",
                    struct(
                        col("guid").alias("SystemGuid"), col("HistorySamples")
                    ).alias("value"),
                )
            )
        else:
            cleaned_pcdm_df = self.data.withColumn(
                "guid", sha2(col("TagName"), 256).cast("string")
            ).withColumn(
                "value",
                struct(
                    col("guid").alias("SystemGuid"),
                    array(
                        struct(
                            col("TagName").alias("ItemName"),
                            lit(self.quality).alias("Quality"),
                            col("EventTime").alias("Time"),
                            col("Value").alias("Value"),
                        ),
                    ).alias("HistorySamples"),
                ),
            )

        df = (
            cleaned_pcdm_df.withColumn(
                "CloudPlatformEvent",
                struct(
                    lit(datetime.now(tz=pytz.UTC)).alias("CreatedTime"),
                    lit(expr("uuid()")).alias("Id"),
                    col("guid").alias("CreatorId"),
                    lit("CloudPlatformSystem").alias("CreatorType"),
                    lit(None).alias("GeneratorId"),
                    lit("CloudPlatformTenant").alias("GeneratorType"),
                    col("guid").alias("TargetId"),
                    lit("CloudPlatformTenant").alias("TargetType"),
                    lit(None).alias("TargetContext"),
                    struct(
                        lit("TextualBody").alias("type"),
                        to_json(col("value")).alias("value"),
                        lit("application/json").alias("format"),
                    ).alias("Body"),
                    array(
                        struct(
                            lit("SystemType").alias("Key"),
                            lit("apm-system").alias("Value"),
                        ),
                        struct(
                            lit("SystemGuid").alias("Key"), col("guid").alias("Value")
                        ),
                    ).alias("BodyProperties"),
                    lit("DataChange.Update").alias("EventType"),
                ),
            )
            .withColumn("AnnotationStreamIds", lit(","))
            .withColumn("partitionKey", col("guid"))
        )
        if self.compress_payload:
            return df.select(
                _compress_payload(to_json("CloudPlatformEvent")).alias(
                    "CloudPlatformEvent"
                ),
                "AnnotationStreamIds",
                "partitionKey",
            )
        else:
            return df.select(
                "CloudPlatformEvent", "AnnotationStreamIds", "partitionKey"
            )
