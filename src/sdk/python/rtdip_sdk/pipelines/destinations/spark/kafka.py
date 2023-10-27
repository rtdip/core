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
from pyspark.sql import DataFrame
from py4j.protocol import Py4JJavaError
from pyspark.sql.functions import to_json, struct

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class SparkKafkaDestination(DestinationInterface):
    """
    This Spark destination class is used to write batch or streaming data from Kafka. Required and optional configurations can be found in the Attributes tables below.

    Additionally, there are more optional configurations which can be found [here.](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html){ target="_blank" }

    For compatibility between Spark and Kafka, the columns in the input dataframe are concatenated into one 'value' column of JSON string.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.destinations import SparkKafkaDestination

    kafka_destination = SparkKafkaDestination(
        data=df,
        options={
            "kafka.bootstrap.servers": "host1:port1,host2:port2"
        },
        trigger="10 seconds",
        query_name="KafkaDestination",
        query_wait_interval=None
    )

    kafka_destination.write_stream()

    OR

    kafka_destination.write_batch()
    ```

    Parameters:
        data (DataFrame): Dataframe to be written to Kafka
        options (dict): A dictionary of Kafka configurations (See Attributes tables below). For more information on configuration options see [here](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html){ target="_blank" }
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None

    The following options must be set for the Kafka destination for both batch and streaming queries.

    Attributes:
        kafka.bootstrap.servers (A comma-separated list of host︰port): The Kafka "bootstrap.servers" configuration. (Streaming and Batch)

    The following configurations are optional:

    Attributes:
        topic (str):Sets the topic that all rows will be written to in Kafka. This option overrides any topic column that may exist in the data. (Streaming and Batch)
        includeHeaders (bool): Whether to include the Kafka headers in the row. (Streaming and Batch)

    """

    data: DataFrame
    options: dict
    trigger: str
    query_name: str
    query_wait_interval: int

    def __init__(
        self,
        data: DataFrame,
        options: dict,
        trigger="10 seconds",
        query_name="KafkaDestination",
        query_wait_interval: int = None,
    ) -> None:
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
        spark_libraries.add_maven_library(get_default_package("spark_sql_kafka"))
        return spark_libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_write_validation(self):
        return True

    def post_write_validation(self):
        return True

    def write_batch(self):
        """
        Writes batch data to Kafka.
        """
        try:
            return (
                self.data.select(to_json(struct("*")).alias("value"))
                .write.format("kafka")
                .options(**self.options)
                .save()
            )

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self):
        """
        Writes steaming data to Kafka.
        """
        try:
            TRIGGER_OPTION = (
                {"availableNow": True}
                if self.trigger == "availableNow"
                else {"processingTime": self.trigger}
            )
            query = (
                self.data.select(to_json(struct("*")).alias("value"))
                .writeStream.trigger(**TRIGGER_OPTION)
                .format("kafka")
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
