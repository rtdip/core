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
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, BinaryType, LongType, MapType, IntegerType, ArrayType

from .models import Libraries
from ..._sdk_utils.compare_versions import _package_version_meets_minimum

class SparkClient():
    spark_configuration: dict
    spark_libraries: Libraries
    spark_session: SparkSession
    spark_remote: str
    
    def __init__(self, spark_configuration: dict, spark_libraries: Libraries, spark_remote: str = None):
        self.spark_configuration = spark_configuration
        self.spark_libraries = spark_libraries
        if spark_remote != None:
            _package_version_meets_minimum("pyspark", "3.4.0")
        self.spark_remote = spark_remote
        self.spark_session = self.get_spark_session()

    def get_spark_session(self) -> SparkSession:

        try:
            temp_spark_configuration = self.spark_configuration.copy()
            
            spark = SparkSession.builder
            spark_app_name = "spark.app.name" 
            spark_master = "spark.master"

            if spark_app_name in temp_spark_configuration:
                spark = spark.appName(temp_spark_configuration[spark_app_name])
                temp_spark_configuration.pop(spark_app_name)

            if spark_master in temp_spark_configuration:
                spark = spark.master(temp_spark_configuration[spark_master])
                temp_spark_configuration.pop(spark_master)

            if self.spark_remote:
                spark = spark.remote(self.spark_remote)
                
            if len(self.spark_libraries.maven_libraries) > 0:
                temp_spark_configuration["spark.jars.packages"] = ','.join(maven_package.to_string() for maven_package in self.spark_libraries.maven_libraries)
                temp_spark_configuration["spark.jars.repositories"] = "https://oss.sonatype.org/content/repositories/iodelta-1080"

            for configuration in temp_spark_configuration.items():
                spark = spark.config(configuration[0], configuration[1])

            spark_session = spark.getOrCreate()
            # TODO: Implemented in DBR 11 but not yet available in pyspark
            # spark_session.streams.addListener(SparkStreamingListener())
            return spark_session

        except Exception as e:
            logging.exception(str(e))
            raise e

def get_dbutils(
    spark: SparkSession,
):  # please note that this function is used in mocking by its name
    try:
        from pyspark.dbutils import DBUtils  # noqa

        if "dbutils" not in locals():
            utils = DBUtils(spark)
            return utils
        else:
            return locals().get("dbutils")
    except ImportError:
        return None
    
# # TODO: Implemented in DBR 11 but not yet available in open source pyspark
# from pyspark.sql.streaming import StreamingQueryListener
# class SparkStreamingListener(StreamingQueryListener):
#     def onQueryStarted(self, event):
#         logging.info("Query started: {} {}".format(event.id, event.name))

#     def onQueryProgress(self, event):
#         logging.info("Query Progress: {}".format(event))

#     def onQueryTerminated(self, event):
#         logging.info("Query terminated: {} {}".format(event.id, event.name))

EVENTHUB_SCHEMA = StructType([
    StructField('body', BinaryType(), True), 
    StructField('partition', StringType(), True), 
    StructField('offset', StringType(), True), 
    StructField('sequenceNumber', LongType(), True), 
    StructField('enqueuedTime', TimestampType(), True), 
    StructField('publisher', StringType(), True), 
    StructField('partitionKey', StringType(), True), 
    StructField('properties', MapType(StringType(), StringType(), True), True), 
    StructField('systemProperties', MapType(StringType(), StringType(), True), True)
])

OPC_PUBLISHER_SCHEMA = StructType([
    StructField('ApplicationUri',StringType(),True),
    StructField('DisplayName',StringType(),True),
    StructField('NodeId', StringType(),True),
    StructField('Value', StructType([
        StructField('SourceTimestamp',StringType(),True),
        StructField('StatusCode', StructType([
            StructField('Code', LongType(),True),
            StructField('Symbol', StringType(),True)
        ]), True),
        StructField('Value',StringType(),True)
    ]),True)
])

PROCESS_DATA_MODEL_SCHEMA = StructType([
    StructField('TagName', StringType(), True), 
    StructField('EventTime', TimestampType(), True), 
    StructField('Status', StringType(), True), 
    StructField('Value', StringType(), True), 
    StructField('ValueType', StringType(), True),
    StructField('ChangeType', StringType(), True)
])

KAFKA_SCHEMA = StructType(
           [StructField('key', BinaryType(), True),
            StructField('value', BinaryType(), True),
            StructField('topic', StringType(), True),
            StructField('partition', IntegerType(), True),
            StructField('offset', LongType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('timestampType', IntegerType(), True)]
       )

KAFKA_EVENTHUB_SCHEMA = StructType(
           [StructField('body', BinaryType(), True),
            StructField('partition', StringType(), True),
            StructField('sequenceNumber', LongType(), True),
            StructField('enqueuedTime', TimestampType(), True),
            StructField('properties', MapType(StringType(), StringType(), True), True)]
       )

KINESIS_SCHEMA = StructType(
           [StructField('partitionKey', StringType(), True),
            StructField('data', BinaryType(), True),
            StructField('stream', StringType(), True),
            StructField('shardId', StringType(), True),
            StructField('sequenceNumber', StringType(), True),
            StructField('approximateArrivalTimestamp', TimestampType(), True)]
       )

FLEDGE_SCHEMA = ArrayType(
            StructType([
            StructField("asset", StringType(), True),
            StructField("readings", MapType(StringType(), StringType(), True), True),
            StructField("timestamp", TimestampType(), True)])
       )

EDGEX_SCHEMA = StructType([
  StructField('apiVersion', StringType(), True),
  StructField('id', StringType(), True),
  StructField('deviceName', StringType(), True),
  StructField('profileName', StringType(), True),
  StructField('sourceName', StringType(), True),
  StructField('origin', LongType(), True),
  StructField('readings', ArrayType(
    StructType([
      StructField('id', StringType(), True),
      StructField('origin', LongType(), True),
      StructField('deviceName', StringType(), True),
      StructField('resourceName', StringType(), True),
      StructField('profileName', StringType(), True),
      StructField('valueType', StringType(), True),
      StructField('value', StringType(), True)]))
  , True)
])