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
from ..._pipeline_utils.spark import EVENTHUB_SCHEMA
from ..._pipeline_utils.constants import get_default_package


class SparkEventhubSource(SourceInterface):
    """
    This Spark source class is used to read batch or streaming data from Eventhubs. Eventhub configurations need to be specified as options in a dictionary.
    Additionally, there are more optional configurations which can be found [here.](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration){ target="_blank" }
    If using startingPosition or endingPosition make sure to check out the **Event Position** section for more details and examples.

    Example
    --------
    ```python
    #Eventhub Source for Streaming Queries

    from rtdip_sdk.pipelines.sources import SparkEventhubSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility
    import json

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    connectionString = "Endpoint=sb://{NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={ACCESS_KEY_NAME};SharedAccessKey={ACCESS_KEY}=;EntityPath={EVENT_HUB_NAME}"

    startingEventPosition = {
    "offset": -1,
    "seqNo": -1,
    "enqueuedTime": None,
    "isInclusive": True
    }

    eventhub_source = SparkEventhubSource(
        spark=spark,
        options = {
            "eventhubs.connectionString": connectionString,
            "eventhubs.consumerGroup": "{YOUR-CONSUMER-GROUP}",
            "eventhubs.startingPosition": json.dumps(startingEventPosition),
            "maxEventsPerTrigger" : 1000
        }
    )

    eventhub_source.read_stream()
    ```
    ```python
     #Eventhub Source for Batch Queries

    from rtdip_sdk.pipelines.sources import SparkEventhubSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility
    import json

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    connectionString = "Endpoint=sb://{NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={ACCESS_KEY_NAME};SharedAccessKey={ACCESS_KEY}=;EntityPath={EVENT_HUB_NAME}"

    startingEventPosition = {
        "offset": -1,
        "seqNo": -1,
        "enqueuedTime": None,
        "isInclusive": True
    }

    endingEventPosition = {
        "offset": None,
        "seqNo": -1,
        "enqueuedTime": endTime,
        "isInclusive": True
    }

    eventhub_source = SparkEventhubSource(
        spark,
        options = {
            "eventhubs.connectionString": connectionString,
            "eventhubs.consumerGroup": "{YOUR-CONSUMER-GROUP}",
            "eventhubs.startingPosition": json.dumps(startingEventPosition),
            "eventhubs.endingPosition": json.dumps(endingEventPosition)
        }
    )

    eventhub_source.read_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session
        options (dict): A dictionary of Eventhub configurations (See Attributes table below)

    Attributes:
        eventhubs.connectionString (str):  Eventhubs connection string is required to connect to the Eventhubs service. (Streaming and Batch)
        eventhubs.consumerGroup (str): A consumer group is a view of an entire eventhub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. (Streaming and Batch)
        eventhubs.startingPosition (JSON str): The starting position for your Structured Streaming job. If a specific EventPosition is not set for a partition using startingPositions, then we use the EventPosition set in startingPosition. If nothing is set in either option, we will begin consuming from the end of the partition. (Streaming and Batch)
        eventhubs.endingPosition: (JSON str): The ending position of a batch query. This works the same as startingPosition. (Batch)
        maxEventsPerTrigger (long): Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. (Stream)

    """

    spark: SparkSession
    options: dict

    def __init__(self, spark: SparkSession, options: dict) -> None:
        self.spark = spark
        self.options = options
        self.schema = EVENTHUB_SCHEMA

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        spark_libraries = Libraries()
        spark_libraries.add_maven_library(get_default_package("spark_azure_eventhub"))
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
        """
        Reads batch data from Eventhubs.
        """
        eventhub_connection_string = "eventhubs.connectionString"
        try:
            if eventhub_connection_string in self.options:
                sc = self.spark.sparkContext
                self.options[eventhub_connection_string] = (
                    sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                        self.options[eventhub_connection_string]
                    )
                )

            return self.spark.read.format("eventhubs").options(**self.options).load()

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def read_stream(self) -> DataFrame:
        """
        Reads streaming data from Eventhubs.
        """
        eventhub_connection_string = "eventhubs.connectionString"
        try:
            if eventhub_connection_string in self.options:
                sc = self.spark.sparkContext
                self.options[eventhub_connection_string] = (
                    sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                        self.options[eventhub_connection_string]
                    )
                )

            return (
                self.spark.readStream.format("eventhubs").options(**self.options).load()
            )

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
