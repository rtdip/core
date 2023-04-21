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
from pyspark.sql.types import StructType, StructField, TimestampType, StringType, BinaryType, LongType, MapType, IntegerType
from .models import Libraries

class SparkClient():
    spark_configuration: dict
    spark_libraries: Libraries
    spark_session: SparkSession
    
    def __init__(self, spark_configuration: dict, spark_libraries: Libraries):
        self.spark_configuration = spark_configuration
        self.spark_libraries = spark_libraries
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

            if len(self.spark_libraries.maven_libraries) > 0:
                temp_spark_configuration["spark.jars.packages"] = ','.join(maven_package.to_string() for maven_package in self.spark_libraries.maven_libraries)
                        
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

OPCUA_SCHEMA = StructType([
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

KAFKA_SCHEMA = StructType(
           [StructField('key', BinaryType(), True),
            StructField('value', BinaryType(), True),
            StructField('topic', StringType(), True),
            StructField('partition', IntegerType(), True),
            StructField('offset', LongType(), True),
            StructField('timestamp', TimestampType(), True),
            StructField('timestampType', IntegerType(), True)]
       )

KINESIS_SCHEMA = StructType(
           [StructField('partitionKey', StringType(), True),
            StructField('data', BinaryType(), True),
            StructField('stream', StringType(), True),
            StructField('shardId', StringType(), True),
            StructField('sequenceNumber', StringType(), True),
            StructField('approximateArrivalTimestamp', TimestampType(), True)]
       )