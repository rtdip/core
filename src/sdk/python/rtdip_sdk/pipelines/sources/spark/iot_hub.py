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
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import DEFAULT_PACKAGES, EVENTHUB_SCHEMA

class SparkEventhubSource(SourceInterface):
    '''
    This Spark source class is used to read batch or streaming data from an IoT Hub. 
    '''
    spark: SparkSession
    options: dict

    def __init__(self, spark: SparkSession, options: dict) -> None:
        self.spark = spark
        self.options = options
        self.schema = EVENTHUB_SCHEMA


    @staticmethod
    def system_type():
        '''
        Attributes:
            SystemType (Environment): Requires PYSPARK
        '''            
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
        Reads batch data from IoT Hubs.
        '''
        eventhub_connection_string = "eventhubs.connectionString"
        try:
            if eventhub_connection_string in self.options:
                sc = self.spark.sparkContext
                self.options[eventhub_connection_string] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(self.options[eventhub_connection_string])

            return (self.spark
                .read
                .format("eventhubs")
                .options(**self.options)
                .load()
            )

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
        
    def read_stream(self) -> DataFrame:
        '''
        Reads streaming data from IoT Hubs.
        '''
        iothub_connection_string = "eventhubs.connectionString"
        try:
            if eventhub_connection_string in self.options:
                sc = self.spark.sparkContext
                self.options[eventhub_connection_string] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(self.options[eventhub_connection_string])

            return (self.spark
                .readStream
                .format("eventhubs")
                .options(**self.options)
                .load()
            )

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e