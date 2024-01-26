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
from pyspark.sql.functions import col, when, date_format, floor
from py4j.protocol import Py4JJavaError

from ..interfaces import DestinationInterface
from ..spark.delta import SparkDeltaDestination
from ..spark.delta_merge import (
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


class SparkPCDMToDeltaDestination(DestinationInterface):
    """
    The Process Control Data Model written to Delta.

    Example
    --------
    ```python
    #PCDM Latest To Delta Destination for Streaming Queries

    from rtdip_sdk.pipelines.destinations import SparkPCDMToDeltaDestination

    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        data=df,
        options={
            "checkpointLocation": "{/CHECKPOINT-LOCATION/}"
        },
        destination_float="{DELTA_TABLE_PATH_FLOAT}",
        destination_string="{DELTA_TABLE_PATH_STRING}",
        destination_integer="{DELTA_TABLE_PATH_INTEGER}",
        mode="append",
        trigger="10 seconds",
        query_name="PCDMToDeltaDestination",
        query_wait_interval=None,
        merge=True,
        try_broadcast_join=False,
        remove_nanoseconds=False,
        remove_duplicates-True
    )

    pcdm_to_delta_destination.write_stream()
    ```
    ```python
    #PCDM Latest To Delta Destination for Batch Queries

    from rtdip_sdk.pipelines.destinations import SparkPCDMToDeltaDestination

    pcdm_to_delta_destination = SparkPCDMToDeltaDestination(
        data=df,
        options={
            "maxRecordsPerFile", "10000"
        },
        destination_float="{DELTA_TABLE_PATH_FLOAT}",
        destination_string="{DELTA_TABLE_PATH_STRING}",
        destination_integer="{DELTA_TABLE_PATH_INTEGER}",
        mode="overwrite",
        trigger="10 seconds",
        query_name="PCDMToDeltaDestination",
        query_wait_interval=None,
        merge=True,
        try_broadcast_join=False,
        remove_nanoseconds=False,
        remove_duplicates-True
    )

    pcdm_to_delta_destination.write_batch()
    ```

    Parameters:
        data (DataFrame): Dataframe to be merged into a Delta Table
        options (dict): Options that can be specified for a Delta Table read operation (See Attributes table below). Further information on the options is available for [batch](https://docs.delta.io/latest/delta-batch.html#write-to-a-table){ target="_blank" } and [streaming](https://docs.delta.io/latest/delta-streaming.html#delta-table-as-a-sink){ target="_blank" }.
        destination_float (str): Either the name of the Hive Metastore or Unity Catalog Delta Table **or** the path to the Delta table to store float values.
        destination_string (Optional str): Either the name of the Hive Metastore or Unity Catalog Delta Table **or** the path to the Delta table to store string values.
        destination_integer (Optional str): Either the name of the Hive Metastore or Unity Catalog Delta Table **or** the path to the Delta table to store integer values
        mode (str): Method of writing to Delta Table - append/overwrite (batch), append/complete (stream)
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None
        merge (bool): Use Delta Merge to perform inserts, updates and deletes
        try_broadcast_join (bool): Attempts to perform a broadcast join in the merge which can leverage data skipping using partition pruning and file pruning automatically. Can fail if dataframe being merged is large and therefore more suitable for streaming merges than batch merges
        remove_nanoseconds (bool): Removes nanoseconds from the EventTime column and replaces with zeros
        remove_duplicates (bool: Removes duplicates before writing the data

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
    """

    spark: SparkSession
    data: DataFrame
    options: dict
    destination_float: str
    destination_string: str
    destination_integer: str
    mode: str
    trigger: str
    query_name: str
    query_wait_interval: int
    merge: bool
    try_broadcast_join: bool
    remove_nanoseconds: bool
    remove_duplicates: bool

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
        options: dict,
        destination_float: str,
        destination_string: str = None,
        destination_integer: str = None,
        mode: str = None,
        trigger="10 seconds",
        query_name: str = "PCDMToDeltaDestination",
        query_wait_interval: int = None,
        merge: bool = True,
        try_broadcast_join=False,
        remove_nanoseconds: bool = False,
        remove_duplicates: bool = True,
    ) -> None:
        self.spark = spark
        self.data = data
        self.destination_float = destination_float
        self.destination_string = destination_string
        self.destination_integer = destination_integer
        self.options = options
        self.mode = mode
        self.trigger = trigger
        self.query_name = query_name
        self.query_wait_interval = query_wait_interval
        self.merge = merge
        self.try_broadcast_join = try_broadcast_join
        self.remove_nanoseconds = remove_nanoseconds
        self.remove_duplicates = remove_duplicates

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

    def _get_eventdate_string(self, df: DataFrame) -> str:
        dates_df = df.select("EventDate").distinct()
        dates_df = dates_df.select(
            date_format("EventDate", "yyyy-MM-dd").alias("EventDate")
        )
        dates_list = list(dates_df.toPandas()["EventDate"])
        return str(dates_list).replace("[", "").replace("]", "")

    def _write_delta_merge(self, df: DataFrame, destination: str):
        df = df.select(
            "EventDate", "TagName", "EventTime", "Status", "Value", "ChangeType"
        )
        when_matched_update_list = [
            DeltaMergeConditionValues(
                condition="(source.ChangeType IN ('insert', 'update', 'upsert')) AND ((source.Status != target.Status) OR (source.Value != target.Value))",
                values={
                    "EventDate": "source.EventDate",
                    "TagName": "source.TagName",
                    "EventTime": "source.EventTime",
                    "Status": "source.Status",
                    "Value": "source.Value",
                },
            )
        ]
        when_matched_delete_list = [
            DeltaMergeCondition(condition="source.ChangeType = 'delete'")
        ]
        when_not_matched_insert_list = [
            DeltaMergeConditionValues(
                condition="(source.ChangeType IN ('insert', 'update', 'upsert'))",
                values={
                    "EventDate": "source.EventDate",
                    "TagName": "source.TagName",
                    "EventTime": "source.EventTime",
                    "Status": "source.Status",
                    "Value": "source.Value",
                },
            )
        ]

        merge_condition = "source.EventDate = target.EventDate AND source.TagName = target.TagName AND source.EventTime = target.EventTime"

        perform_merge = True
        if self.try_broadcast_join != True:
            eventdate_string = self._get_eventdate_string(df)
            if eventdate_string == None or eventdate_string == "":
                perform_merge = False
            else:
                merge_condition = (
                    "target.EventDate in ({}) AND ".format(eventdate_string)
                    + merge_condition
                )

        if perform_merge == True:
            SparkDeltaMergeDestination(
                spark=self.spark,
                data=df,
                destination=destination,
                options=self.options,
                merge_condition=merge_condition,
                when_matched_update_list=when_matched_update_list,
                when_matched_delete_list=when_matched_delete_list,
                when_not_matched_insert_list=when_not_matched_insert_list,
                try_broadcast_join=self.try_broadcast_join,
                trigger=self.trigger,
                query_name=self.query_name,
            ).write_batch()

    def _write_delta_batch(self, df: DataFrame, destination: str):
        if self.merge == True:
            if "EventDate" not in df.columns:
                df = df.withColumn("EventDate", date_format("EventTime", "yyyy-MM-dd"))

            self._write_delta_merge(
                df.filter(col("ChangeType").isin("insert", "update", "upsert")),
                destination,
            )
            self._write_delta_merge(
                df.filter(col("ChangeType") == "delete"), destination
            )
        else:
            df = df.select("TagName", "EventTime", "Status", "Value")
            SparkDeltaDestination(
                data=df,
                destination=destination,
                options=self.options,
                mode=self.mode,
                trigger=self.trigger,
                query_name=self.query_name,
            ).write_batch()

    def _write_data_by_type(self, df: DataFrame):
        if self.merge == True:
            df = df.withColumn(
                "ChangeType",
                when(df["ChangeType"].isin("insert", "update"), "upsert").otherwise(
                    df["ChangeType"]
                ),
            )

        if self.remove_nanoseconds == True:
            df = df.withColumn(
                "EventTime",
                (floor(col("EventTime").cast("double") * 1000) / 1000).cast(
                    "timestamp"
                ),
            )

        if self.remove_duplicates == True:
            df = df.drop_duplicates(["TagName", "EventTime", "ChangeType"])

        float_df = df.filter(ValueTypeConstants.FLOAT_VALUE).withColumn(
            "Value", col("Value").cast("float")
        )
        self._write_delta_batch(float_df, self.destination_float)

        if self.destination_string != None:
            string_df = df.filter(ValueTypeConstants.STRING_VALUE)
            self._write_delta_batch(string_df, self.destination_string)

        if self.destination_integer != None:
            integer_df = df.filter(ValueTypeConstants.INTEGER_VALUE).withColumn(
                "Value", col("Value").cast("integer")
            )
            self._write_delta_batch(integer_df, self.destination_integer)

    def _write_stream_microbatches(self, df: DataFrame, epoch_id=None):  # NOSONAR
        df.persist()
        self._write_data_by_type(df)
        df.unpersist()

    def write_batch(self):
        """
        Writes Process Control Data Model data to Delta
        """
        try:
            if self.try_broadcast_join != True:
                self.data.persist()

            self._write_data_by_type(self.data)

            if self.try_broadcast_join != True:
                self.data.unpersist()

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
            if self.merge == True:
                query = (
                    self.data.writeStream.trigger(**TRIGGER_OPTION)
                    .format("delta")
                    .foreachBatch(self._write_stream_microbatches)
                    .queryName(self.query_name)
                    .outputMode("update")
                    .options(**self.options)
                    .start()
                )
            else:
                default_checkpoint_location = None
                float_checkpoint_location = None
                string_checkpoint_location = None
                integer_checkpoint_location = None

                append_options = self.options.copy()
                if "checkpointLocation" in self.options:
                    default_checkpoint_location = self.options["checkpointLocation"]
                    if default_checkpoint_location[-1] != "/":
                        default_checkpoint_location += "/"
                    float_checkpoint_location = default_checkpoint_location + "float"
                    string_checkpoint_location = default_checkpoint_location + "string"
                    integer_checkpoint_location = (
                        default_checkpoint_location + "integer"
                    )

                if float_checkpoint_location is not None:
                    append_options["checkpointLocation"] = float_checkpoint_location

                delta_float = SparkDeltaDestination(
                    data=self.data.select("TagName", "EventTime", "Status", "Value")
                    .filter(ValueTypeConstants.FLOAT_VALUE)
                    .withColumn("Value", col("Value").cast("float")),
                    destination=self.destination_float,
                    options=append_options,
                    mode=self.mode,
                    trigger=self.trigger,
                    query_name=self.query_name + "_float",
                )

                delta_float.write_stream()

                if self.destination_string != None:
                    if string_checkpoint_location is not None:
                        append_options["checkpointLocation"] = (
                            string_checkpoint_location
                        )

                    delta_string = SparkDeltaDestination(
                        data=self.data.select(
                            "TagName", "EventTime", "Status", "Value"
                        ).filter(ValueTypeConstants.STRING_VALUE),
                        destination=self.destination_string,
                        options=append_options,
                        mode=self.mode,
                        trigger=self.trigger,
                        query_name=self.query_name + "_string",
                    )

                    delta_string.write_stream()

                if self.destination_integer != None:
                    if integer_checkpoint_location is not None:
                        append_options["checkpointLocation"] = (
                            integer_checkpoint_location
                        )

                    delta_integer = SparkDeltaDestination(
                        data=self.data.select("TagName", "EventTime", "Status", "Value")
                        .filter(ValueTypeConstants.INTEGER_VALUE)
                        .withColumn("Value", col("Value").cast("integer")),
                        destination=self.destination_integer,
                        options=append_options,
                        mode=self.mode,
                        trigger=self.trigger,
                        query_name=self.query_name + "_integer",
                    )

                    delta_integer.write_stream()

                if self.query_wait_interval:
                    while self.spark.streams.active != []:
                        for query in self.spark.streams.active:
                            if query.lastProgress:
                                logging.info(
                                    "{}: {}".format(query.name, query.lastProgress)
                                )
                        time.sleep(self.query_wait_interval)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
