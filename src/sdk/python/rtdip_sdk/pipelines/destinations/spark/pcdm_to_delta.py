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
from pyspark.sql.functions import col, when, date_format
from py4j.protocol import Py4JJavaError

from ..interfaces import DestinationInterface
from ..spark.delta import SparkDeltaDestination
from ..spark.delta_merge import SparkDeltaMergeDestination, DeltaMergeCondition, DeltaMergeConditionValues
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package

class ValueTypeConstants():
    INTEGER_VALUE = "ValueType = 'integer'"
    FLOAT_VALUE = "ValueType = 'float'"
    STRING_VALUE = "ValueType = 'string'"


class SparkPCDMToDeltaDestination(DestinationInterface):
    '''
    The Process Control Data Model written to Delta

    Args:
        data (DataFrame): Dataframe to be merged into a Delta Table
        options (dict): Options that can be specified for a Delta Table read operation (See Attributes table below). Further information on the options is available for [batch](https://docs.delta.io/latest/delta-batch.html#write-to-a-table){ target="_blank" } and [streaming](https://docs.delta.io/latest/delta-streaming.html#delta-table-as-a-sink){ target="_blank" }.
        destination_float (str): Either the name of the Hive Metastore or Unity Catalog Delta Table **or** the path to the Delta table to store float values.
        destination_string (str): Either the name of the Hive Metastore or Unity Catalog Delta Table **or** the path to the Delta table to store string values.
        destination_integer (Optional str): Either the name of the Hive Metastore or Unity Catalog Delta Table **or** the path to the Delta table to store integer values
        mode (str): Method of writing to Delta Table - append/overwrite (batch), append/complete (stream)
        trigger (str): Frequency of the write operation
        query_name (str): Unique name for the query in associated SparkSession
        merge (bool): Use Delta Merge to perform inserts, updates and deletes
        try_broadcast_join (bool): Attempts to perform a broadcast join in the merge which can leverage data skipping using partition pruning and file pruning automatically. Can fail if dataframe being merged is large and therefore more suitable for streaming merges than batch merges
        remove_duplicates (bool: Removes duplicates before writing the data 

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
    '''
    spark: SparkSession
    data: DataFrame
    options: dict    
    destination_float: str
    destination_string: str
    destination_integer: str
    mode: str
    trigger: str
    query_name: str
    merge: bool
    try_broadcast_join: bool
    remove_duplicates: bool

    def __init__(self, 
                 spark: SparkSession, 
                 data: DataFrame, 
                 options: dict,
                 destination_float: str,
                 destination_string: str,
                 destination_integer: str = None,
                 mode: str = None,
                 trigger="10 seconds",
                 query_name: str ="PCDMToDeltaMergeDestination",
                 merge: bool = True,
                 try_broadcast_join = False,
                 remove_duplicates: bool = True) -> None: 
        self.spark = spark
        self.data = data
        self.destination_float = destination_float
        self.destination_string = destination_string
        self.destination_integer = destination_integer
        self.options = options
        self.mode = mode
        self.trigger = trigger
        self.query_name = query_name
        self.merge = merge
        self.try_broadcast_join = try_broadcast_join
        self.remove_duplicates = remove_duplicates

    @staticmethod
    def system_type():
        '''
        Attributes:
            SystemType (Environment): Requires PYSPARK
        '''             
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
        dates_df = dates_df.select(date_format("EventDate", "yyyy-MM-dd").alias("EventDate"))
        dates_list = list(dates_df.toPandas()["EventDate"])
        return str(dates_list).replace('[','').replace(']','')

    def _write_delta_batch(self, df: DataFrame, destination: str):
        
        if self.merge == True:
            df = df.select("EventDate", "TagName", "EventTime", "Status", "Value", "ChangeType")
            when_matched_update_list = [
                DeltaMergeConditionValues(
                    condition="(source.ChangeType IN ('insert', 'update', 'upsert')) AND ((source.Status != target.Status) OR (source.Value != target.Value))",
                    values={
                        "EventDate": "source.EventDate", 
                        "TagName": "source.TagName",
                        "EventTime": "source.EventTime",
                        "Status": "source.Status",
                        "Value": "source.Value"
                    }
                )
            ]
            when_matched_delete_list = [
                DeltaMergeCondition(
                    condition="source.ChangeType = 'delete'"
                )
            ]
            when_not_matched_insert_list = [
                DeltaMergeConditionValues(
                    condition="(source.ChangeType IN ('insert', 'update', 'upsert'))",
                    values={
                        "EventDate": "source.EventDate", 
                        "TagName": "source.TagName",
                        "EventTime": "source.EventTime",
                        "Status": "source.Status",
                        "Value": "source.Value"
                    }
                )
            ]

            merge_condition = "source.EventDate = target.EventDate AND source.TagName = target.TagName AND source.EventTime = target.EventTime"
            
            perform_merge = True
            if self.try_broadcast_join != True:
                eventdate_string = self._get_eventdate_string(df)
                if eventdate_string == None or eventdate_string == "":
                    perform_merge = False
                else:
                    merge_condition = "target.EventDate in ({}) AND ".format(eventdate_string) + merge_condition

            if perform_merge == True:
                delta = SparkDeltaMergeDestination(
                    spark=self.spark,
                    data=df,
                    destination=destination,
                    options=self.options,
                    merge_condition=merge_condition,
                    when_matched_update_list=when_matched_update_list,
                    when_matched_delete_list=when_matched_delete_list,
                    when_not_matched_insert_list=when_not_matched_insert_list,
                    try_broadcast_join=self.try_broadcast_join
                )
        else:
            df = df.select("TagName", "EventTime", "Status", "Value")
            delta = SparkDeltaDestination(
                data=df,
                destination=destination,
                options=self.options
            )
        
        delta.write_batch()

    def _write_data_by_type(self, df: DataFrame):
        if self.merge == True:
            df = df.withColumn("ChangeType", when(df["ChangeType"].isin("insert", "update"), "upsert").otherwise(df["ChangeType"]))

        if self.remove_duplicates == True:
            df = df.drop_duplicates(["TagName", "EventTime"])

        float_df = (
            df
            .filter(ValueTypeConstants.FLOAT_VALUE)
            .withColumn("Value", col("Value").cast("float"))
        )
        self._write_delta_batch(float_df, self.destination_float)

        string_df = df.filter(ValueTypeConstants.STRING_VALUE)
        self._write_delta_batch(string_df, self.destination_string)

        if self.destination_integer != None:
            integer_df = (
                df
                .filter(ValueTypeConstants.INTEGER_VALUE)
                .withColumn("Value", col("Value").cast("integer"))
            )
            self._write_delta_batch(integer_df, self.destination_integer)         

    def _write_stream_microbatches(self, df: DataFrame, epoch_id = None): # NOSONAR
        df.persist()
        self._write_data_by_type(df)
        df.unpersist()

    def write_batch(self):
        '''
        Writes Process Control Data Model data to Delta
        '''
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
        '''
        Writes streaming Process Control Data Model data to Delta using foreachBatch
        '''
        try:
            if self.merge == True:
                query = (
                    self.data
                    .writeStream
                    .trigger(processingTime=self.trigger)
                    .format("delta")
                    .foreachBatch(self._write_stream_microbatches)
                    .queryName(self.query_name)
                    .outputMode("update")
                    .options(**self.options)
                    .start()
                )
            else:
                delta_float = SparkDeltaDestination(
                    data=self.data.filter(ValueTypeConstants.FLOAT_VALUE).withColumn("Value", col("Value").cast("float")),
                    destination=self.destination_float,
                    options=self.options,
                    mode=self.mode,
                    trigger=self.trigger,
                    query_name=self.query_name + "_float"
                )
            
                delta_float.write_stream()                

                delta_string = SparkDeltaDestination(
                    data=self.data.filter(ValueTypeConstants.STRING_VALUE),
                    destination=self.destination_string,
                    options=self.options,
                    mode=self.mode,
                    trigger=self.trigger,
                    query_name=self.query_name + "_string"
                )
            
                delta_string.write_stream()

                if self.destination_integer != None:
                    delta_integer = SparkDeltaDestination(
                        data=self.data.filter(ValueTypeConstants.INTEGER_VALUE),
                        destination=self.destination_integer,
                        options=self.options,
                        mode=self.mode,
                        trigger=self.trigger,
                        query_name=self.query_name + "_integer"
                    )
                
                    delta_integer.write_stream()

                while self.spark.streams.active != []:
                    for query in self.spark.streams.active:
                        if query.lastProgress:
                            logging.info("{}: {}".format(query.name, query.lastProgress))
                    time.sleep(10)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e