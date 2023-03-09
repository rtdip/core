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

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, MavenLibrary, SystemType
from ..._pipeline_utils.constants import DEFAULT_PACKAGES

class SparkDeltaSource(SourceInterface):
    '''
    The Spark Delta Source is used to read data from a Delta table. 

    The connection class represents a connection to a database and uses the Databricks SQL Connector API's for Python to intereact with cluster/jobs.
    To find details for SQL warehouses server_hostname and http_path location to the SQL Warehouse tab in the documentation.
    Args:
        spark: Spark Session required to read data from a Delta table
        options: Options that can be specified for a Delta Table read operation (See Attributes table below). Further information on the options available is [here](https://docs.delta.io/latest/delta-streaming.html#delta-table-as-a-source)
        table_name: Name of the Hive Metastore or Unity Catalog Delta Table

    Attributes:
        maxFilesPerTrigger (int): How many new files to be considered in every micro-batch. The default is 1000.
        maxBytesPerTrigger (int): How much data gets processed in each micro-batch.
        ignoreDeletes (bool str): ignore transactions that delete data at partition boundaries.
        ignoreChanges (bool str): re-process updates if files had to be rewritten in the source table due to a data changing operation.
        startingVersion (int str): The Delta Lake version to start from.
        startingTimestamp (datetime str): The timestamp to start from.
        withEventTimeOrder (bool str): Whether the initial snapshot should be processed with event time order.
        timestampAsOf (datetime str): Query the Delta Table from a specific point in time.
        versionAsOf (int str): Query the Delta Table from a specific version.
    ''' 
    spark: SparkSession
    options: dict
    table_name: str

    def __init__(self, spark: SparkSession, options: dict, table_name: str) -> None:
        self.spark = spark
        self.options = options
        self.table_name = table_name

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_maven_library(DEFAULT_PACKAGES["spark_delta_core"])
        return libraries
    
    @staticmethod
    def settings() -> dict:
        return {}
    
    def pre_read_validation(self):
        return True
    
    def post_read_validation(self):
        return True

    def read_batch(self):
        '''
        Reads batch data from Delta.
        '''
        try:
            return (self.spark
                .read
                .format("delta")
                .options(**self.options)
                .table(self.table_name)
            )

        except Exception as e:
            logging.exception('error with spark read batch delta function', e.__traceback__)
            raise e
        
    def read_stream(self) -> DataFrame:
        '''
        Reads streaming data from Delta.
        '''
        try:
            return (self.spark
                .readStream
                .format("delta")
                .options(**self.options)
                .load(self.table_name)
            )

        except Exception as e:
            logging.exception('error with spark read stream delta function', e.__traceback__)
            raise e