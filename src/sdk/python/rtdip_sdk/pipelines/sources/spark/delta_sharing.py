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
from py4j.protocol import Py4JJavaError

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class SparkDeltaSharingSource(SourceInterface):
    """
    The Spark Delta Sharing Source is used to read data from a Delta table where Delta sharing is configured

    Example
    --------
    ```python
    #Delta Sharing Source for Streaming Queries

    from rtdip_sdk.pipelines.sources import SparkDeltaSharingSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    delta_sharing_source = SparkDeltaSharingSource(
        spark=spark,
        options={
            "maxFilesPerTrigger": 1000,
            "ignoreChanges: True,
            "startingVersion": 0
        },
        table_name="{YOUR-DELTA-TABLE-PATH}"
    )

    delta_sharing_source.read_stream()
    ```
    ```python
    #Delta Sharing Source for Batch Queries

    from rtdip_sdk.pipelines.sources import SparkDeltaSharingSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    delta_sharing_source = SparkDeltaSharingSource(
        spark=spark,
        options={
            "versionAsOf": 0,
            "timestampAsOf": "yyyy-mm-dd hh:mm:ss[.fffffffff]"
        },
        table_name="{YOUR-DELTA-TABLE-PATH}"
    )

    delta_sharing_source.read_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session required to read data from a Delta table
        options (dict): Options that can be specified for a Delta Table read operation (See Attributes table below). Further information on the options is available [here](https://docs.databricks.com/data-sharing/read-data-open.html#apache-spark-read-shared-data){ target="_blank" }
        table_path (str): Path to credentials file and Delta table to query

    Attributes:
        ignoreDeletes (bool str): Ignore transactions that delete data at partition boundaries. (Streaming)
        ignoreChanges (bool str): Pre-process updates if files had to be rewritten in the source table due to a data changing operation. (Streaming)
        startingVersion (int str): The Delta Lake version to start from. (Streaming)
        startingTimestamp (datetime str): The timestamp to start from. (Streaming)
        maxFilesPerTrigger (int): How many new files to be considered in every micro-batch. The default is 1000. (Streaming)
        maxBytesPerTrigger (int): How much data gets processed in each micro-batch. (Streaming)
        readChangeFeed (bool str): Stream read the change data feed of the shared table. (Batch & Streaming)
        timestampAsOf (datetime str): Query the Delta Table from a specific point in time. (Batch)
        versionAsOf (int str): Query the Delta Table from a specific version. (Batch)
    """

    spark: SparkSession
    options: dict
    table_path: str

    def __init__(self, spark: SparkSession, options: dict, table_path: str) -> None:
        self.spark = spark
        self.options = options
        self.table_path = table_path

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
        libraries.add_maven_library(get_default_package("spark_delta_sharing"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_read_validation(self):
        return True

    def post_read_validation(self):
        return True

    def read_batch(self):
        """
        Reads batch data from Delta. Most of the options provided by the Apache Spark DataFrame read API are supported for performing batch reads on Delta tables.
        """
        try:
            return (
                self.spark.read.format("deltaSharing")
                .options(**self.options)
                .table(self.table_path)
            )

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def read_stream(self) -> DataFrame:
        """
        Reads streaming data from Delta. All of the data in the table is processed as well as any new data that arrives after the stream started. .load() can take table name or path.
        """
        try:
            return (
                self.spark.readStream.format("deltaSharing")
                .options(**self.options)
                .load(self.table_path)
            )

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
