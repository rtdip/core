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
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StringType, BinaryType
from pyspark.sql.functions import col, struct, to_json
from pyspark.sql.types import StringType, BinaryType

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class SparkEventhubDestination(DestinationInterface):
    """
    This Spark destination class is used to write batch or streaming data to Eventhubs. Eventhub configurations need to be specified as options in a dictionary.
    Additionally, there are more optional configurations which can be found [here.](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration){ target="_blank" }
    If using startingPosition or endingPosition make sure to check out **Event Position** section for more details and examples.

    Examples
    --------
    ```python
    #Eventhub Destination for Streaming Queries

    from rtdip_sdk.pipelines.destinations import SparkEventhubDestination
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    connectionString = Endpoint=sb://{NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={ACCESS_KEY_NAME};SharedAccessKey={ACCESS_KEY}=;EntityPath={EVENT_HUB_NAME}

    eventhub_destination = SparkEventhubDestination(
        spark=spark,
        data=df,
        options={
            "eventhubs.connectionString": connectionString,
            "eventhubs.consumerGroup": "{YOUR-EVENTHUB-CONSUMER-GROUP}",
            "checkpointLocation": "/{CHECKPOINT-LOCATION}/"
        },
        trigger="10 seconds",
        query_name="EventhubDestination",
        query_wait_interval=None
    )

    eventhub_destination.write_stream()
    ```
    ```python
    #Eventhub Destination for Batch Queries

    from rtdip_sdk.pipelines.destinations import SparkEventhubDestination
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    connectionString = Endpoint=sb://{NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={ACCESS_KEY_NAME};SharedAccessKey={ACCESS_KEY}=;EntityPath={EVENT_HUB_NAME}


    eventhub_destination = SparkEventhubDestination(
        spark=spark,
        data=df,
        options={
            "eventhubs.connectionString": connectionString,
            "eventhubs.consumerGroup": "{YOUR-EVENTHUB-CONSUMER-GROUP}"
        },
        trigger="10 seconds",
        query_name="EventhubDestination",
        query_wait_interval=None
    )

    eventhub_destination.write_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session
        data (DataFrame): Dataframe to be written to Eventhub
        options (dict): A dictionary of Eventhub configurations (See Attributes table below). All Configuration options for Eventhubs can be found [here.](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration){ target="_blank" }
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
        eventhubs.connectionString (str):  Eventhubs connection string is required to connect to the Eventhubs service. (Streaming and Batch)
        eventhubs.consumerGroup (str): A consumer group is a view of an entire eventhub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. (Streaming and Batch)
        eventhubs.startingPosition (JSON str): The starting position for your Structured Streaming job. If a specific EventPosition is not set for a partition using startingPositions, then we use the EventPosition set in startingPosition. If nothing is set in either option, we will begin consuming from the end of the partition. (Streaming and Batch)
        eventhubs.endingPosition: (JSON str): The ending position of a batch query. This works the same as startingPosition. (Batch)
        maxEventsPerTrigger (long): Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. (Stream)
    """

    spark: SparkSession
    data: DataFrame
    options: dict
    trigger: str
    query_name: str
    query_wait_interval: int

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
        options: dict,
        trigger="10 seconds",
        query_name="EventhubDestination",
        query_wait_interval: int = None,
    ) -> None:
        self.spark = spark
        self.data = data
        self.options = options
        self.trigger = trigger
        self.query_name = query_name
        self.query_wait_interval = query_wait_interval

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

    def pre_write_validation(self):
        return True

    def post_write_validation(self):
        return True

    def prepare_columns(self):
        if "body" in self.data.columns:
            if self.data.schema["body"].dataType not in [StringType(), BinaryType()]:
                try:
                    self.data.withColumn("body", col("body").cast(StringType()))
                except Exception as e:
                    raise ValueError(
                        "'body' column must be of string or binary type", e
                    )
        else:
            self.data = self.data.withColumn(
                "body",
                to_json(
                    struct(
                        [
                            col(column).alias(column)
                            for column in self.data.columns
                            if column not in ["partitionId", "partitionKey"]
                        ]
                    )
                ),
            )
        for column in self.data.schema:
            if (
                column.name in ["partitionId", "partitionKey"]
                and column.dataType != StringType()
            ):
                try:
                    self.data = self.data.withColumn(
                        column.name, col(column.name).cast(StringType())
                    )
                except Exception as e:
                    raise ValueError(f"Column {column.name} must be of string type", e)
        return self.data.select(
            [
                column
                for column in self.data.columns
                if column in ["partitionId", "partitionKey", "body"]
            ]
        )

    def write_batch(self):
        """
        Writes batch data to Eventhubs.
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
            df = self.prepare_columns()
            return df.write.format("eventhubs").options(**self.options).save()

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self):
        """
        Writes steaming data to Eventhubs.
        """
        eventhub_connection_string = "eventhubs.connectionString"
        try:
            TRIGGER_OPTION = (
                {"availableNow": True}
                if self.trigger == "availableNow"
                else {"processingTime": self.trigger}
            )
            if eventhub_connection_string in self.options:
                sc = self.spark.sparkContext
                self.options[eventhub_connection_string] = (
                    sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(
                        self.options[eventhub_connection_string]
                    )
                )
            df = self.prepare_columns()
            df = self.data.select(
                [
                    column
                    for column in self.data.columns
                    if column in ["partitionId", "partitionKey", "body"]
                ]
            )
            query = (
                df.writeStream.trigger(**TRIGGER_OPTION)
                .format("eventhubs")
                .options(**self.options)
                .queryName(self.query_name)
                .start()
            )

            if self.query_wait_interval:
                while query.isActive:
                    if query.lastProgress:
                        logging.info(query.lastProgress)
                    time.sleep(self.query_wait_interval)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
