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
from dotenv import load_dotenv
from src.sdk.python.rtdip_sdk.pipelines.interfaces import PipelineComponentBaseInterface
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType

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

            if "spark.app.name" in temp_spark_configuration:
                spark = spark.appName(temp_spark_configuration["spark.app.name"])
                temp_spark_configuration.pop("spark.app.name")

            if "spark.master" in temp_spark_configuration:
                spark = spark.master(temp_spark_configuration["spark.master"])
                temp_spark_configuration.pop("spark.master")

            if len(self.spark_libraries.maven_libraries) > 0:
                temp_spark_configuration["spark.jars.packages"] = ','.join(maven_package.to_string() for maven_package in self.spark_libraries.maven_libraries)
                        
            for configuration in temp_spark_configuration.items():
                spark = spark.config(configuration[0], configuration[1])

            spark_session = spark.getOrCreate()
            # TODO: Implemented in DBR 11 but not yet available in pyspark
            # spark_session.streams.addListener(SparkStreamingListener())
            return spark_session

        except Exception as e:
            logging.exception('error with spark session function', e.__traceback__)
            raise e

# # TODO: Implemented in DBR 11 but not yet available in open source pyspark
# from pyspark.sql.streaming import StreamingQueryListener
# class SparkStreamingListener(StreamingQueryListener):
#     def onQueryStarted(self, event):
#         logging.info("Query started: {} {}".format(event.id, event.name))

#     def onQueryProgress(self, event):
#         logging.info("Query Progress: {}".format(event))

#     def onQueryTerminated(self, event):
#         logging.info("Query terminated: {} {}".format(event.id, event.name))