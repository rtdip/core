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
from pyspark.sql.functions import col, map_from_entries
from urllib.parse import urlparse

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import KAFKA_EVENTHUB_SCHEMA
from ..._pipeline_utils.constants import get_default_package

class SparkKafkaEventhubSource(SourceInterface):
    '''
    This Spark source class is used to read batch or streaming data from an Eventhub using the Kafka protocol. This enables Eventhubs to be used as a source in applications like Delta Live Tables or Databricks Serverless Jobs as the Spark Eventhubs JAR is not supported in this scenarios.
    
    The dataframe returned is transformed to ensure the schema is as close to the Eventhub Spark source as possible. There are some minor differences:
    
    - `offset` is not included in the Kafka source and therefore is not available in the returned Dataframe
    - `publisher` is not included in the Kafka source and therefore is not available in the returned Dataframe
    - `partitionKey` is not included in the Kafka source and therefore is not available in the returned Dataframe
    - `systemProperties` and `properties` are merged in `properties` in the returned Dataframe as Kafka Headers returns them all in the same column with no way to differentiate between them

    Default settings will be specified if not provided in the `options` parameter:

    - `kafka.sasl.mechanism` will be set to `PLAIN`
    - `kafka.security.protocol` will be set to `SASL_SSL`
    - `kafka.request.timeout.ms` will be set to `60000`
    - `kafka.session.timeout.ms` will be set to `60000`

    Required and optional configurations can be found in the Attributes tables below. 
    Additionally, there are more optional configurations which can be found [here.](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html){ target="_blank" }
    
    Args:
        spark (SparkSession): Spark Session
        options (dict): A dictionary of Kafka configurations (See Attributes tables below). For more information on configuration options see [here](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html){ target="_blank" }
        connection_string (str): Eventhubs connection string is required to connect to the Eventhubs service. This must include the Eventhub name as the `EntityPath` parameter. Example `"Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test_key;EntityPath=test_eventhub"`
        consumer_group (str): The Eventhub consumer group to use for the connection

    The following options are the most common configurations for Kafka. 
    
    The only configuration that must be set for the Kafka source for both batch and streaming queries is listed below.
 
    Attributes:
        kafka.bootstrap.servers (A comma-separated list of host︰port):  The Kafka "bootstrap.servers" configuration. (Streaming and Batch)

    There are multiple ways of specifying which topics to subscribe to. You should provide only one of these parameters:
     
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

    '''
    def __init__(self, spark: SparkSession, options: dict, connection_string: str, consumer_group: str) -> None:
        self.spark = spark
        self.options = options
        self.connection_string = connection_string
        self.consumer_group = consumer_group
        self.connection_string_properties = self._parse_connection_string(connection_string)
        self.schema = KAFKA_EVENTHUB_SCHEMA
        self.options = self._configure_options(options)


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
            connection_string += "SharedAccessKey=" + properties.get("shared_access_key") + ";"

        if properties.get("shared_access_key_name"):
            connection_string += "SharedAccessKeyName=" + properties.get("shared_access_key_name") + ";"

        if properties.get("shared_access_signature"):
            connection_string += "SharedAccessSignature=" + properties.get("shared_access_signature") + ";"
        return connection_string
    
    def _configure_options(self, options: dict) -> dict:
        if "subscribe" not in options:
            options["subscribe"] = self.connection_string_properties.get("eventhub_name")
        
        if "kafka.bootstrap.servers" not in options:
            options["kafka.bootstrap.servers"] = self.connection_string_properties.get("fully_qualified_namespace") + ":9093"

        if "kafka.sasl.mechanism" not in options:
            options["kafka.sasl.mechanism"] = "PLAIN"

        if "kafka.security.protocol" not in options:
            options["kafka.security.protocol"] = "SASL_SSL"

        if "kafka.sasl.jaas.config" not in options:
            kafka_package = "org.apache.kafka.common.security.plain.PlainLoginModule"
            if "DATABRICKS_RUNTIME_VERSION" in os.environ:
                kafka_package = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"
            connection_string = self._connection_string_builder(self.connection_string_properties)
            options["kafka.sasl.jaas.config"] = "{} required username=\"$ConnectionString\" password=\"{}\";".format(kafka_package, connection_string) # NOSONAR

        if "kafka.request.timeout.ms" not in options:
            options["kafka.request.timeout.ms"] = "60000"

        if "kafka.session.timeout.ms" not in options:
            options["kafka.session.timeout.ms"] = "60000"

        if "kafka.group.id" not in options:
            options["kafka.group.id"] = self.consumer_group

        if "includeHeaders" not in options:
            options["includeHeaders"] = "true"
        
        return options

    def _transform_to_eventhub_schema(self, df: DataFrame) -> DataFrame:
        return (df
            .withColumn("headers", map_from_entries(col("headers")))
            .select(
                col("value").alias("body"),
                col("partition").cast("string"),
                col("offset").alias("sequenceNumber"),
                col("timestamp").alias("enqueuedTime"),
                col("headers").alias("properties").cast("map<string,string>")
            )
        )
    
    def read_batch(self) -> DataFrame:
        '''
        Reads batch data from Kafka.
        '''
        try:
            df = (self.spark
                .read
                .format("kafka")
                .options(**self.options)
                .load()
            )
            return self._transform_to_eventhub_schema(df)
        
        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
        
    def read_stream(self) -> DataFrame:
        '''
        Reads streaming data from Kafka.
        '''
        try:
            df = (self.spark
                .readStream
                .format("kafka")
                .options(**self.options)
                .load()
            )
            return self._transform_to_eventhub_schema(df)
        
        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e