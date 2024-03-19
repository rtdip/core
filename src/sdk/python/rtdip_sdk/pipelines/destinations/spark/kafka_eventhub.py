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

import os
import logging
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, struct, to_json, array
from urllib.parse import urlparse
from pyspark.sql.types import (
    StringType,
    BinaryType,
    ArrayType,
    IntegerType,
    StructType,
    StructField,
)
import time

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package
from ..._pipeline_utils.amqp import decode_kafka_headers_to_amqp_properties


class SparkKafkaEventhubDestination(DestinationInterface):
    """
    This Spark Destination class is used to write batch or streaming data to an Eventhub using the Kafka protocol. This enables Eventhubs to be used as a destination in applications like Delta Live Tables or Databricks Serverless Jobs as the Spark Eventhubs JAR is not supported in these scenarios.

    Default settings will be specified if not provided in the `options` parameter:

    - `kafka.sasl.mechanism` will be set to `PLAIN`
    - `kafka.security.protocol` will be set to `SASL_SSL`
    - `kafka.request.timeout.ms` will be set to `60000`
    - `kafka.session.timeout.ms` will be set to `60000`

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.destinations import SparkKafkaEventhubDestination
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    connectionString = Endpoint=sb://{NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={ACCESS_KEY_NAME};SharedAccessKey={ACCESS_KEY}=;EntityPath={EVENT_HUB_NAME}

    eventhub_destination = SparkKafkaEventhubDestination(
        spark=spark,
        data=df,
        options={
            "kafka.bootstrap.servers": "host1:port1,host2:port2"
        },
        connection_string="{YOUR-EVENTHUB-CONNECTION-STRING}",
        consumer_group="{YOUR-EVENTHUB-CONSUMER-GROUP}",
        trigger="10 seconds",
        query_name="KafkaEventhubDestination",
        query_wait_interval=None
    )

    eventhub_destination.write_stream()

    OR

    eventhub_destination.write_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session
        data (DataFrame): Any columns not listed in the required schema [here](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html#writing-data-to-kafka){ target="_blank" } will be merged into a single column named "value", or ignored if "value" is an existing column
        connection_string (str): Eventhubs connection string is required to connect to the Eventhubs service. This must include the Eventhub name as the `EntityPath` parameter. Example `"Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test_key;EntityPath=test_eventhub"`
        options (dict): A dictionary of Kafka configurations (See Attributes tables below)
        consumer_group (str): The Eventhub consumer group to use for the connection
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (optional str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None

    The following are commonly used parameters that may be included in the options dict. kafka.bootstrap.servers is the only required config. A full list of configs can be found [here](https://kafka.apache.org/documentation/#producerconfigs){ target="_blank" }

    Attributes:
        kafka.bootstrap.servers (A comma-separated list of host︰port):  The Kafka "bootstrap.servers" configuration. (Streaming and Batch)
        topic (string): Required if there is no existing topic column in your DataFrame. Sets the topic that all rows will be written to in Kafka. (Streaming and Batch)
        includeHeaders (bool): Determines whether to include the Kafka headers in the row; defaults to False. (Streaming and Batch)
    """

    spark: SparkSession
    data: DataFrame
    connection_string: str
    options: dict
    consumer_group: str
    trigger: str
    query_name: str
    connection_string_properties: dict
    query_wait_interval: int

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
        connection_string: str,
        options: dict,
        consumer_group: str,
        trigger: str = "10 seconds",
        query_name: str = "KafkaEventhubDestination",
        query_wait_interval: int = None,
    ) -> None:
        self.spark = spark
        self.data = data
        self.connection_string = connection_string
        self.options = options
        self.consumer_group = consumer_group
        self.trigger = trigger
        self.query_name = query_name
        self.connection_string_properties = self._parse_connection_string(
            connection_string
        )
        self.options = self._configure_options(options)
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

    def pre_write_validation(self) -> bool:
        return True

    def post_write_validation(self) -> bool:
        return True

    # Code is from Azure Eventhub Python SDK. Will import the package if possible with Conda in the  conda-forge channel in the future
    def _parse_connection_string(self, connection_string: str):
        conn_settings = [s.split("=", 1) for s in connection_string.split(";")]
        if any(len(tup) != 2 for tup in conn_settings):
            raise ValueError("Connection string is either blank or malformed.")
        conn_settings = dict(conn_settings)
        shared_access_signature = None
        for key, value in conn_settings.items():
            if key.lower() == "sharedaccesssignature":
                shared_access_signature = value
        shared_access_key = conn_settings.get("SharedAccessKey")
        shared_access_key_name = conn_settings.get("SharedAccessKeyName")
        if any([shared_access_key, shared_access_key_name]) and not all(
            [shared_access_key, shared_access_key_name]
        ):
            raise ValueError(
                "Connection string must have both SharedAccessKeyName and SharedAccessKey."
            )
        if shared_access_signature is not None and shared_access_key is not None:
            raise ValueError(
                "Only one of the SharedAccessKey or SharedAccessSignature must be present."
            )
        endpoint = conn_settings.get("Endpoint")
        if not endpoint:
            raise ValueError("Connection string is either blank or malformed.")
        parsed = urlparse(endpoint.rstrip("/"))
        if not parsed.netloc:
            raise ValueError("Invalid Endpoint on the Connection String.")
        namespace = parsed.netloc.strip()
        properties = {
            "fully_qualified_namespace": namespace,
            "endpoint": endpoint,
            "eventhub_name": conn_settings.get("EntityPath"),
            "shared_access_signature": shared_access_signature,
            "shared_access_key_name": shared_access_key_name,
            "shared_access_key": shared_access_key,
        }
        return properties

    def _connection_string_builder(self, properties: dict) -> str:
        connection_string = "Endpoint=" + properties.get("endpoint") + ";"

        if properties.get("shared_access_key"):
            connection_string += (
                "SharedAccessKey=" + properties.get("shared_access_key") + ";"
            )

        if properties.get("shared_access_key_name"):
            connection_string += (
                "SharedAccessKeyName=" + properties.get("shared_access_key_name") + ";"
            )

        if properties.get("shared_access_signature"):
            connection_string += (
                "SharedAccessSignature="
                + properties.get("shared_access_signature")
                + ";"
            )
        return connection_string

    def _configure_options(self, options: dict) -> dict:
        if "topic" not in options:
            options["topic"] = self.connection_string_properties.get("eventhub_name")

        if "kafka.bootstrap.servers" not in options:
            options["kafka.bootstrap.servers"] = (
                self.connection_string_properties.get("fully_qualified_namespace")
                + ":9093"
            )

        if "kafka.sasl.mechanism" not in options:
            options["kafka.sasl.mechanism"] = "PLAIN"

        if "kafka.security.protocol" not in options:
            options["kafka.security.protocol"] = "SASL_SSL"

        if "kafka.sasl.jaas.config" not in options:
            kafka_package = "org.apache.kafka.common.security.plain.PlainLoginModule"
            if "DATABRICKS_RUNTIME_VERSION" in os.environ or (
                "_client" in self.spark.__dict__
                and "databricks" in self.spark.client.host
            ):
                kafka_package = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
            connection_string = self._connection_string_builder(
                self.connection_string_properties
            )
            options["kafka.sasl.jaas.config"] = (
                '{} required username="$ConnectionString" password="{}";'.format(
                    kafka_package, connection_string
                )
            )  # NOSONAR

        if "kafka.request.timeout.ms" not in options:
            options["kafka.request.timeout.ms"] = "60000"

        if "kafka.session.timeout.ms" not in options:
            options["kafka.session.timeout.ms"] = "60000"

        if "kafka.group.id" not in options:
            options["kafka.group.id"] = self.consumer_group

        options["includeHeaders"] = "true"

        return options

    def _transform_to_eventhub_schema(self, df: DataFrame) -> DataFrame:
        column_list = ["key", "headers", "topic", "partition"]
        if "value" not in df.columns:
            df = df.withColumn(
                "value",
                to_json(
                    struct(
                        [
                            col(column).alias(column)
                            for column in df.columns
                            if column not in column_list
                        ]
                    )
                ),
            )
        if "headers" in df.columns and (
            df.schema["headers"].dataType.elementType["key"].nullable == True
            or df.schema["headers"].dataType.elementType["value"].nullable == True
        ):
            raise ValueError("key and value in the headers column cannot be nullable")

        return df.select(
            [
                column
                for column in df.columns
                if column in ["value", "key", "headers", "topic", "partition"]
            ]
        )

    def write_batch(self) -> DataFrame:
        """
        Reads batch data from Kafka.
        """
        try:
            df = self._transform_to_eventhub_schema(self.data)
            df.write.format("kafka").options(**self.options).save()

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self) -> DataFrame:
        """
        Reads streaming data from Kafka.
        """
        try:
            df = self._transform_to_eventhub_schema(self.data)
            TRIGGER_OPTION = (
                {"availableNow": True}
                if self.trigger == "availableNow"
                else {"processingTime": self.trigger}
            )
            query = (
                df.writeStream.trigger(**TRIGGER_OPTION)
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
