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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, BinaryType, StringType, LongType, TimestampType, MapType

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, MavenLibrary, SystemType
from ..._pipeline_utils.constants import DEFAULT_PACKAGES

class SparkEventhubSource(SourceInterface):
    '''
    '''
    spark: SparkSession
    options: dict

    def __init__(self, spark: SparkSession, options: dict) -> None:
        self.spark = spark
        self.options = options
        self.schema = StructType(
            [StructField('body', BinaryType(), True), 
             StructField('partition', StringType(), True), 
             StructField('offset', StringType(), True), 
             StructField('sequenceNumber', LongType(), True), 
             StructField('enqueuedTime', TimestampType(), True), 
             StructField('publisher', StringType(), True), 
             StructField('partitionKey', StringType(), True), 
             StructField('properties', MapType(StringType(), StringType(), True), True), 
             StructField('systemProperties', MapType(StringType(), StringType(), True), True)]
        )


    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        spark_libraries = Libraries()
        spark_libraries.add_maven_library(DEFAULT_PACKAGES["spark_azure_eventhub"])
        return spark_libraries
    
    @staticmethod
    def settings() -> dict:
        return {}
    
    def pre_read_validation(self) -> bool:
        return True
    
    def post_read_validation(self, df: DataFrame) -> bool:
        assert df.schema == self.schema
        return True

    def read_batch(self) -> DataFrame:
        '''
        '''
        try:
            if "eventhubs.connectionString" in self.options:
                sc = self.spark.sparkContext
                self.options["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(self.options["eventhubs.connectionString"])

            return (self.spark
                .read
                .format("eventhubs")
                .options(**self.options)
                .load()
            )

        except Exception as e:
            print(e)
            logging.exception("error with spark read batch eventhub function")
            raise e
        
    def read_stream(self) -> DataFrame:
        try:
            if "eventhubs.connectionString" in self.options:
                sc = self.spark.sparkContext
                self.options["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(self.options["eventhubs.connectionString"])

            return (self.spark
                .readStream
                .format("eventhubs")
                .options(**self.options)
                .load()
            )

        except Exception as e:
            print(e)
            logging.exception("error with spark read stream eventhub function")
            raise e