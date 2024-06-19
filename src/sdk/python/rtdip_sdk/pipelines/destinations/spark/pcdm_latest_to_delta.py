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

import logging
import time
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, struct, max
from pyspark.sql import Window
from py4j.protocol import Py4JJavaError

from ..interfaces import DestinationInterface
from .delta import SparkDeltaDestination
from .delta_merge import (
    SparkDeltaMergeDestination,
    DeltaMergeCondition,
    DeltaMergeConditionValues,
)
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class ValueTypeConstants:
    INTEGER_VALUE = "ValueType = 'integer'"
    FLOAT_VALUE = "ValueType = 'float'"
    STRING_VALUE = "ValueType = 'string'"


class SparkPCDMLatestToDeltaDestination(DestinationInterface):
    """
    The Process Control Data Model Latest Values written to Delta.

    Example
    --------
    ```python
    #PCDM Latest To Delta Destination for Streaming Queries

    from rtdip_sdk.pipelines.destinations import SparkPCDMLatestToDeltaDestination

    pcdm_latest_to_delta_destination = SparkPCDMLatestToDeltaDestination(
        data=df,
        options={
            "checkpointLocation": "{/CHECKPOINT-LOCATION/}"
        },
        destination="{DELTA_TABLE_PATH}",
        mode="append",
        trigger="10 seconds",
        query_name="PCDMLatestToDeltaDestination",
        query_wait_interval=None
    )

    pcdm_latest_to_delta_destination.write_stream()
    ```
    ```python
    #PCDM Latest To Delta Destination for Batch Queries

    from rtdip_sdk.pipelines.destinations import SparkPCDMLatestToDeltaDestination

    pcdm_latest_to_delta_destination = SparkPCDMLatestToDeltaDestination(
        data=df,
        options={
            "maxRecordsPerFile", "10000"
        },
        destination="{DELTA_TABLE_PATH}",
        mode="overwrite",
        trigger="10 seconds",
        query_name="PCDMLatestToDeltaDestination",
        query_wait_interval=None
    )

    pcdm_latest_to_delta_destination.write_batch()
    ```

    Parameters:
        data (DataFrame): Dataframe to be merged into a Delta Table
        options (dict): Options that can be specified for a Delta Table read operation (See Attributes table below). Further information on the options is available for [batch](https://docs.delta.io/latest/delta-batch.html#write-to-a-table){ target="_blank" } and [streaming](https://docs.delta.io/latest/delta-streaming.html#delta-table-as-a-sink){ target="_blank" }.
        destination (str): Either the name of the Hive Metastore or Unity Catalog Delta Table **or** the path to the Delta table to store the latest values
        mode (str): Method of writing to Delta Table - append/overwrite (batch), append/complete (stream)
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
    """

    spark: SparkSession
    data: DataFrame
    options: dict
    destination: str
    mode: str
    trigger: str
    query_name: str
    query_wait_interval: int

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
        options: dict,
        destination: str,
        mode: str = None,
        trigger="10 seconds",
        query_name: str = "PCDMLatestToDeltaDestination",
        query_wait_interval: int = None,
    ) -> None:
        self.spark = spark
        self.data = data
        self.destination = destination
        self.options = options
        self.mode = mode
        self.trigger = trigger
        self.query_name = query_name
        self.query_wait_interval = query_wait_interval

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
        libraries.add_maven_library(get_default_package("spark_delta_core"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_write_validation(self):
        return True

    def post_write_validation(self):
        return True

    def _write_latest_to_delta(self, df: DataFrame, epoch_id=None):  # NOSONAR
        df.persist()

        latest_df = (
            df.withColumn(
                "Latest",
                max(struct("EventTime", "Status")).over(Window.partitionBy("TagName")),
            )
            .withColumn(
                "GoodLatest",
                when(
                    col("Latest.Status") == "Good",
                    struct(col("EventTime"), col("Value"), col("ValueType")),
                ).otherwise(
                    max(
                        when(
                            col("Status") == "Good",
                            struct("EventTime", "Value", "ValueType"),
                        )
                    ).over(Window.partitionBy("TagName"))
                ),
            )
            .filter(col("EventTime") == col("Latest.EventTime"))
            .drop("Latest")
            .dropDuplicates(["TagName"])
        )

        when_matched_update_list = [
            DeltaMergeConditionValues(
                condition="source.EventTime > target.EventTime AND (source.GoodLatest.EventTime IS NULL OR source.GoodLatest.EventTime <= target.GoodEventTime)",
                values={
                    "EventTime": "source.EventTime",
                    "Status": "source.Status",
                    "Value": "source.Value",
                    "ValueType": "source.ValueType",
                },
            ),
            DeltaMergeConditionValues(
                condition="source.EventTime > target.EventTime AND (source.GoodLatest.EventTime IS NOT NULL AND (source.GoodLatest.EventTime > target.GoodEventTime OR target.GoodEventTime IS NULL))",
                values={
                    "EventTime": "source.EventTime",
                    "Status": "source.Status",
                    "Value": "source.Value",
                    "ValueType": "source.ValueType",
                    "GoodEventTime": "source.GoodLatest.EventTime",
                    "GoodValue": "source.GoodLatest.Value",
                    "GoodValueType": "source.GoodLatest.ValueType",
                },
            ),
            DeltaMergeConditionValues(
                condition="source.EventTime <= target.EventTime AND (source.GoodLatest.EventTime IS NOT NULL AND (source.GoodLatest.EventTime > target.GoodEventTime OR target.GoodEventTime IS NULL))",
                values={
                    "GoodEventTime": "source.GoodLatest.EventTime",
                    "GoodValue": "source.GoodLatest.Value",
                    "GoodValueType": "source.GoodLatest.ValueType",
                },
            ),
        ]

        when_not_matched_insert_list = [
            DeltaMergeConditionValues(
                values={
                    "TagName": "source.TagName",
                    "EventTime": "source.EventTime",
                    "Status": "source.Status",
                    "Value": "source.Value",
                    "ValueType": "source.ValueType",
                    "GoodEventTime": "source.GoodLatest.EventTime",
                    "GoodValue": "source.GoodLatest.Value",
                    "GoodValueType": "source.GoodLatest.ValueType",
                },
            )
        ]

        merge_condition = "source.TagName = target.TagName"

        SparkDeltaMergeDestination(
            spark=self.spark,
            data=latest_df,
            destination=self.destination,
            options=self.options,
            merge_condition=merge_condition,
            when_matched_update_list=when_matched_update_list,
            when_not_matched_insert_list=when_not_matched_insert_list,
            trigger=self.trigger,
            query_name=self.query_name,
        ).write_batch()

        df.unpersist()

    def write_batch(self):
        """
        Writes Process Control Data Model data to Delta
        """
        try:
            self._write_latest_to_delta(self.data)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self):
        """
        Writes streaming Process Control Data Model data to Delta using foreachBatch
        """
        try:
            TRIGGER_OPTION = (
                {"availableNow": True}
                if self.trigger == "availableNow"
                else {"processingTime": self.trigger}
            )

            query = (
                self.data.writeStream.trigger(**TRIGGER_OPTION)
                .format("delta")
                .foreachBatch(self._write_latest_to_delta)
                .queryName(self.query_name)
                .outputMode("append")
                .options(**self.options)
                .start()
            )

            if self.query_wait_interval:
                while query.isActive:
                    if query.lastProgress:
                        logging.info(query.lastProgress)
                    time.sleep(self.query_wait_interval)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
