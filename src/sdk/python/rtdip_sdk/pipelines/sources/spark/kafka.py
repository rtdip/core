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
from ..._pipeline_utils.spark import KAFKA_SCHEMA
from ..._pipeline_utils.constants import get_default_package


class SparkKafkaSource(SourceInterface):
    """
    This Spark source class is used to read batch or streaming data from Kafka. Required and optional configurations can be found in the Attributes tables below.

    Additionally, there are more optional configurations which can be found [here.](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html){ target="_blank" }

    Example
    --------
    ```python
     #Kafka Source for Streaming Queries

    from rtdip_sdk.pipelines.sources import SparkKafkaSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    kafka_source = SparkKafkaSource(
        spark=spark,
        options={
            "kafka.bootstrap.servers": "{HOST_1}:{PORT_1},{HOST_2}:{PORT_2}",
            "subscribe": "{TOPIC_1},{TOPIC_2}",
            "includeHeaders", "true"
        }
    )

    kafka_source.read_stream()
    ```
    ```python
     #Kafka Source for Batch Queries

    from rtdip_sdk.pipelines.sources import SparkKafkaSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    kafka_source = SparkKafkaSource(
        spark=spark,
        options={
            "kafka.bootstrap.servers": "{HOST_1}:{PORT_1},{HOST_2}:{PORT_2}",
            "subscribe": "{TOPIC_1},{TOPIC_2}",
            "startingOffsets": "earliest",
            "endingOffsets": "latest"
        }
    )

    kafka_source.read_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session
        options (dict): A dictionary of Kafka configurations (See Attributes tables below). For more information on configuration options see [here](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html){ target="_blank" }

    The following attributes are the most common configurations for Kafka.

    The only configuration that must be set for the Kafka source for both batch and streaming queries is listed below.

    Attributes:
        kafka.bootstrap.servers (A comma-separated list of host︰port):  The Kafka "bootstrap.servers" configuration. (Streaming and Batch)

    There are multiple ways of specifying which topics to subscribe to. You should provide only one of these attributes:

    Attributes:
        assign (json string {"topicA"︰[0,1],"topicB"︰[2,4]}):  Specific TopicPartitions to consume. Only one of "assign", "subscribe" or "subscribePattern" options can be specified for Kafka source. (Streaming and Batch)
        subscribe (A comma-separated list of topics): The topic list to subscribe. Only one of "assign", "subscribe" or "subscribePattern" options can be specified for Kafka source. (Streaming and Batch)
        subscribePattern (Java regex string): The pattern used to subscribe to topic(s). Only one of "assign, "subscribe" or "subscribePattern" options can be specified for Kafka source. (Streaming and Batch)

    The following configurations are optional:

    Attributes:
        startingTimestamp (timestamp str): The start point of timestamp when a query is started, a string specifying a starting timestamp for all partitions in topics being subscribed. Please refer the note on starting timestamp offset options below. (Streaming and Batch)
        startingOffsetsByTimestamp (JSON str): The start point of timestamp when a query is started, a json string specifying a starting timestamp for each TopicPartition. Please refer the note on starting timestamp offset options below. (Streaming and Batch)
        startingOffsets ("earliest", "latest" (streaming only), or JSON string): The start point when a query is started, either "earliest" which is from the earliest offsets, "latest" which is just from the latest offsets, or a json string specifying a starting offset for each TopicPartition. In the json, -2 as an offset can be used to refer to earliest, -1 to latest.
        endingTimestamp (timestamp str): The end point when a batch query is ended, a json string specifying an ending timestamp for all partitions in topics being subscribed. Please refer the note on ending timestamp offset options below. (Batch)
        endingOffsetsByTimestamp (JSON str): The end point when a batch query is ended, a json string specifying an ending timestamp for each TopicPartition. Please refer the note on ending timestamp offset options below. (Batch)
        endingOffsets (latest or JSON str): The end point when a batch query is ended, either "latest" which is just referred to the latest, or a json string specifying an ending offset for each TopicPartition. In the json, -1 as an offset can be used to refer to latest, and -2 (earliest) as an offset is not allowed. (Batch)
        maxOffsetsPerTrigger (long): Rate limit on maximum number of offsets processed per trigger interval. The specified total number of offsets will be proportionally split across topicPartitions of different volume. (Streaming)
        minOffsetsPerTrigger (long): Minimum number of offsets to be processed per trigger interval. The specified total number of offsets will be proportionally split across topicPartitions of different volume. (Streaming)
        failOnDataLoss (bool): Whether to fail the query when it's possible that data is lost (e.g., topics are deleted, or offsets are out of range). This may be a false alarm. You can disable it when it doesn't work as you expected.
        minPartitions (int): Desired minimum number of partitions to read from Kafka. By default, Spark has a 1-1 mapping of topicPartitions to Spark partitions consuming from Kafka. (Streaming and Batch)
        includeHeaders (bool): Whether to include the Kafka headers in the row. (Streaming and Batch)

    !!! note "Starting Timestamp Offset Note"
        If Kafka doesn't return the matched offset, the behavior will follow to the value of the option <code>startingOffsetsByTimestampStrategy</code>.

        <code>startingTimestamp</code> takes precedence over <code>startingOffsetsByTimestamp</code> and </code>startingOffsets</code>.

        For streaming queries, this only applies when a new query is started, and that resuming will always pick up from where the query left off. Newly discovered partitions during a query will start at earliest.

    !!! note "Ending Timestamp Offset Note"
        If Kafka doesn't return the matched offset, the offset will be set to latest.

        <code>endingOffsetsByTimestamp</code> takes precedence over <code>endingOffsets</code>.

    """

    spark: SparkSession
    options: dict

    def __init__(self, spark: SparkSession, options: dict) -> None:
        self.spark = spark
        self.options = options
        self.schema = KAFKA_SCHEMA

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
        spark_libraries.add_maven_library(get_default_package("spark_sql_kafka"))
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
        Reads batch data from Kafka.
        """
        try:
            return self.spark.read.format("kafka").options(**self.options).load()

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def read_stream(self) -> DataFrame:
        """
        Reads streaming data from Kafka.
        """
        try:
            return self.spark.readStream.format("kafka").options(**self.options).load()

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
