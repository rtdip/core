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
from typing import List, Optional, Union
from pydantic.v1 import BaseModel
from pyspark.sql.functions import broadcast
from pyspark.sql import DataFrame, SparkSession
from py4j.protocol import Py4JJavaError
from delta.tables import DeltaTable, DeltaMergeBuilder

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ...._sdk_utils.compare_versions import _package_version_meets_minimum
from ..._pipeline_utils.constants import get_default_package


class DeltaMergeConditionValues(BaseModel):
    condition: Optional[str]
    values: Union[dict, str]


class DeltaMergeCondition(BaseModel):
    condition: Optional[str]


class SparkDeltaMergeDestination(DestinationInterface):
    """
    The Spark Delta Merge Destination is used to merge data into a Delta table. Refer to this [documentation](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge&language-python) for more information about Delta Merge.

    Examples
    --------
    ```python
    #Delta Merge Destination for Streaming Queries

    from rtdip_sdk.pipelines.destinations import SparkDeltaMergeDestination

    delta_merge_destination = SparkDeltaMergeDestination(
        data=df,
        destination="DELTA-TABLE-PATH",
        options={
            "checkpointLocation": "/{CHECKPOINT-LOCATION}/"
        },
        merge_condition="`source.id = target.id`"
        when_matched_update_list=None
        when_matched_delete_list=None
        when_not_matched_insert_list=None
        when_not_matched_by_source_update_list=None
        when_not_matched_by_source_delete_list=None
        try_broadcast_join=False
        trigger="10 seconds",
        query_name="DeltaDestination"
        query_wait_interval=None
    )

    delta_merge_destination.write_stream()
    ```
    ```python
    #Delta Merge Destination for Batch Queries

    from rtdip_sdk.pipelines.destinations import SparkDeltaMergeDestination

    delta_merge_destination = SparkDeltaMergeDestination(
        data=df,
        destination="DELTA-TABLE-PATH",
        options={},
        merge_condition="`source.id = target.id`",
        when_matched_update_list=None,
        when_matched_delete_list=None,
        when_not_matched_insert_list=None,
        when_not_matched_by_source_update_list=None,
        when_not_matched_by_source_delete_list=None,
        try_broadcast_join=False,
        trigger="10 seconds",
        query_name="DeltaDestination"
        query_wait_interval=None
    )

    delta_merge_destination.write_batch()
    ```

    Parameters:
        data (DataFrame): Dataframe to be merged into a Delta Table
        destination (str): Either the name of the Hive Metastore or Unity Catalog Delta Table **or** the path to the Delta table
        options (dict): Options that can be specified for a Delta Table read operation (See Attributes table below). Further information on the options is available for [batch](https://docs.delta.io/latest/delta-batch.html#write-to-a-table){ target="_blank" } and [streaming](https://docs.delta.io/latest/delta-streaming.html#delta-table-as-a-sink){ target="_blank" }.
        merge_condition (str): Condition for matching records between dataframe and delta table. Reference Dataframe columns as `source` and Delta Table columns as `target`. For example `source.id = target.id`.
        when_matched_update_list (optional list[DeltaMergeConditionValues]): Conditions(optional) and values to be used when updating rows that match the `merge_condition`. Specify `*` for Values if all columns from Dataframe should be inserted.
        when_matched_delete_list (optional list[DeltaMergeCondition]): Conditions(optional) to be used when deleting rows that match the `merge_condition`.
        when_not_matched_insert_list (optional list[DeltaMergeConditionValues]): Conditions(optional) and values to be used when inserting rows that do not match the `merge_condition`. Specify `*` for Values if all columns from Dataframe should be inserted.
        when_not_matched_by_source_update_list (optional list[DeltaMergeConditionValues]): Conditions(optional) and values to be used when updating rows that do not match the `merge_condition`.
        when_not_matched_by_source_delete_list (optional list[DeltaMergeCondition]): Conditions(optional) to be used when deleting rows that do not match the `merge_condition`.
        try_broadcast_join (optional bool): Attempts to perform a broadcast join in the merge which can leverage data skipping using partition pruning and file pruning automatically. Can fail if dataframe being merged is large and therefore more suitable for streaming merges than batch merges
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (optional str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
    """

    spark: SparkSession
    data: DataFrame
    destination: str
    options: dict
    merge_condition: str
    when_matched_update_list: List[DeltaMergeConditionValues]
    when_matched_delete_list: List[DeltaMergeCondition]
    when_not_matched_insert_list: List[DeltaMergeConditionValues]
    when_not_matched_by_source_update_list: List[DeltaMergeConditionValues]
    when_not_matched_by_source_delete_list: List[DeltaMergeCondition]
    try_broadcast_join: bool
    trigger: str
    query_name: str
    query_wait_interval: int

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
        destination: str,
        options: dict,
        merge_condition: str,
        when_matched_update_list: List[DeltaMergeConditionValues] = None,
        when_matched_delete_list: List[DeltaMergeCondition] = None,
        when_not_matched_insert_list: List[DeltaMergeConditionValues] = None,
        when_not_matched_by_source_update_list: List[DeltaMergeConditionValues] = None,
        when_not_matched_by_source_delete_list: List[DeltaMergeCondition] = None,
        try_broadcast_join: bool = False,
        trigger="10 seconds",
        query_name: str = "DeltaMergeDestination",
        query_wait_interval: int = None,
    ) -> None:
        self.spark = spark
        self.data = data
        self.destination = destination
        self.options = options
        self.merge_condition = merge_condition
        self.when_matched_update_list = (
            [] if when_matched_update_list is None else when_matched_update_list
        )
        self.when_matched_delete_list = (
            [] if when_matched_delete_list is None else when_matched_delete_list
        )
        self.when_not_matched_insert_list = (
            [] if when_not_matched_insert_list is None else when_not_matched_insert_list
        )
        if (
            isinstance(when_not_matched_by_source_update_list, list)
            and len(when_not_matched_by_source_update_list) > 0
        ):
            _package_version_meets_minimum("delta-spark", "2.3.0")
        self.when_not_matched_by_source_update_list = (
            []
            if when_not_matched_by_source_update_list is None
            else when_not_matched_by_source_update_list
        )
        if (
            isinstance(when_not_matched_by_source_delete_list, list)
            and len(when_not_matched_by_source_delete_list) > 0
        ):
            _package_version_meets_minimum("delta-spark", "2.3.0")
        self.when_not_matched_by_source_delete_list = (
            []
            if when_not_matched_by_source_delete_list is None
            else when_not_matched_by_source_delete_list
        )
        self.try_broadcast_join = try_broadcast_join
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
        return {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
        }

    def pre_write_validation(self):
        return True

    def post_write_validation(self):
        return True

    def _delta_merge_builder(
        self, df: DataFrame, try_broadcast_join: bool
    ) -> DeltaMergeBuilder:
        if "/" in self.destination:
            delta_table = DeltaTable.forPath(self.spark, self.destination)
        else:
            delta_table = DeltaTable.forName(self.spark, self.destination)

        if try_broadcast_join == True:
            delta_merge_builder = delta_table.alias("target").merge(
                source=broadcast(df).alias("source"), condition=self.merge_condition
            )
        else:
            delta_merge_builder = delta_table.alias("target").merge(
                source=df.alias("source"), condition=self.merge_condition
            )

        for when_matched_update in self.when_matched_update_list:
            if when_matched_update.values == "*":
                delta_merge_builder = delta_merge_builder.whenMatchedUpdateAll(
                    condition=when_matched_update.condition,
                )
            else:
                delta_merge_builder = delta_merge_builder.whenMatchedUpdate(
                    condition=when_matched_update.condition,
                    set=when_matched_update.values,
                )

        for when_matched_delete in self.when_matched_delete_list:
            delta_merge_builder = delta_merge_builder.whenMatchedDelete(
                condition=when_matched_delete.condition,
            )

        for when_not_matched_insert in self.when_not_matched_insert_list:
            if when_not_matched_insert.values == "*":
                delta_merge_builder = delta_merge_builder.whenNotMatchedInsertAll(
                    condition=when_not_matched_insert.condition,
                )
            else:
                delta_merge_builder = delta_merge_builder.whenNotMatchedInsert(
                    condition=when_not_matched_insert.condition,
                    values=when_not_matched_insert.values,
                )

        for (
            when_not_matched_by_source_update
        ) in self.when_not_matched_by_source_update_list:
            delta_merge_builder = delta_merge_builder.whenNotMatchedBySourceUpdate(
                condition=when_not_matched_by_source_update.condition,
                set=when_not_matched_by_source_update.values,
            )

        for (
            when_not_matched_by_source_delete
        ) in self.when_not_matched_by_source_delete_list:
            delta_merge_builder = delta_merge_builder.whenNotMatchedBySourceDelete(
                condition=when_not_matched_by_source_delete.condition,
            )

        return delta_merge_builder

    def _stream_merge_micro_batch(
        self, micro_batch_df: DataFrame, epoch_id=None
    ):  # NOSONAR
        micro_batch_df.persist()

        retry_delta_merge = False

        if self.try_broadcast_join == True:
            try:
                delta_merge = self._delta_merge_builder(
                    micro_batch_df, self.try_broadcast_join
                )
                delta_merge.execute()
            except Exception as e:
                if "SparkOutOfMemoryError" in str(e):
                    retry_delta_merge = True
                else:
                    raise e

        if self.try_broadcast_join == False or retry_delta_merge == True:
            delta_merge = self._delta_merge_builder(micro_batch_df, False)
            delta_merge.execute()

        micro_batch_df.unpersist()

    def write_batch(self):
        """
        Merges batch data into a Delta Table.
        """
        try:
            delta_merge = self._delta_merge_builder(self.data, self.try_broadcast_join)
            return delta_merge.execute()

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self):
        """
        Merges streaming data to Delta using foreachBatch
        """
        TRIGGER_OPTION = (
            {"availableNow": True}
            if self.trigger == "availableNow"
            else {"processingTime": self.trigger}
        )
        try:
            query = (
                self.data.writeStream.trigger(**TRIGGER_OPTION)
                .format("delta")
                .foreachBatch(self._stream_merge_micro_batch)
                .queryName(self.query_name)
                .outputMode("update")
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
