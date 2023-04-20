# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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
from ..._pipeline_utils.constants import DEFAULT_PACKAGES, KINESIS_SCHEMA
class SparkKinesisSource(SourceInterface):
   '''
   The Spark Kinesis Source is used to read data from Amazon Kinesis Data Streams.
   Args:
       spark: Spark Session required to read data from Kinesis
       options: Options that can be specified for a Kinesis read operation (See Attributes table below). Further information on the options is available [here](https://docs.databricks.com/structured-streaming/kinesis.html#configuration){ target="_blank" }
   Attributes:
       awsAccessKey (str): AWS access key.
       awsSecretKey (str): AWS secret access key corresponding to the access key.
       streamName (List[str]): The stream names to subscribe to.
       region (str): The region the streams are defined in.
       endpoint (str): The regional endpoint for Kinesis Data Streams.
       initialPosition (str): The point to start reading from.
   '''
   spark: SparkSession
   options: dict
   def __init__(self, spark: SparkSession, options: dict) -> None:
       self.spark = spark
       self.options = options
       self.schema = KINESIS_SCHEMA
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
       return libraries
   @staticmethod
   def settings() -> dict:
       return {}
   def pre_read_validation(self):
       return True
   def post_read_validation(self, df: DataFrame) -> bool:
        assert df.schema == self.schema
        return True
   def read_batch(self):
       '''
       Reads batch data from Kinesis.
       '''
       try:
           return (self.spark
               .read
               .format("kinesis")
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
       Reads streaming data from Kinesis. All of the data in the table is processed as well as any new data that arrives after the stream started.
       '''
       try:
           return (self.spark
               .readStream
               .format("kinesis")
               .options(**self.options)
               .load()
           )
       except Py4JJavaError as e:
           logging.exception(e.errmsg)
           raise e
       except Exception as e:
           logging.exception(str(e))
           raise e