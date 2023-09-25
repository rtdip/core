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
    to_json,
    col,
    struct,
    lit,
    array,
    monotonically_increasing_id,
    floor,
    row_number,
    collect_list,
    expr,
)
from pyspark.sql import Window
from datetime import datetime
import pytz

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import EDGEX_SCHEMA


class PCDMToHoneywellAPMTransformer(TransformerInterface):
    """
    Converts a Spark Dataframe in PCDM format to Honeywell APM format.
    Args:
        data (Dataframe): Spark Dataframe in PCDM format
        quality (str): Value for quality inside HistorySamples
        history_samples_per_message (int): The number of HistorySamples for each row in the DataFrame (Batch Only)

    """

    data: DataFrame
    quality: str
    history_samples_per_message: int

    def __init__(
        self,
        data: DataFrame,
        quality: str = "Good",
        history_samples_per_message: int = 1,
    ) -> None:
        self.data = data
        self.quality = quality
        self.history_samples_per_message = history_samples_per_message

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
        if self.data.isStreaming == False and self.history_samples_per_message > 1:
            pcdm_df = self.data.withColumn("counter", monotonically_increasing_id())
            w = Window.orderBy("counter")
            cleaned_pcdm_df = (
                pcdm_df.withColumn(
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
                .groupBy("index")
                .agg(collect_list("HistorySamples").alias("HistorySamples"))
                .withColumn("guid", expr("uuid()"))
                .withColumn(
                    "value",
                    struct(
                        col("guid").alias("SystemGuid"), col("HistorySamples")
                    ).alias("value"),
                )
            )
        else:
            cleaned_pcdm_df = self.data.withColumn("guid", expr("uuid()")).withColumn(
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

        df = cleaned_pcdm_df.withColumn(
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
                    struct(lit("SystemGuid").alias("Key"), col("guid").alias("Value")),
                ).alias("BodyProperties"),
                lit("DataChange.Update").alias("EventType"),
            ),
        ).withColumn("AnnotationStreamIds", lit(","))

        return df.select("CloudPlatformEvent", "AnnotationStreamIds")
