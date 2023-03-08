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

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, MavenLibrary, SystemType
from ..._pipeline_utils.constants import DEFAULT_PACKAGES

class SparkEventhubDestination(DestinationInterface):
    '''
    This Spark destination class is used to write batch or streaming data to Event Hubs. Event Hub configurations need to be specified as options in a dictionary.
    Additionally, there are more optional configuration which can be found [here.](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration)
    If using startingPosition or endingPosition make sure to check out **Event Position** section for more details and examples.
    Args:
        options: A dictionary of Event Hub configurations (See Attributes table below)

    Attributes:
        eventhubs.connectionString (str):  Event Hubs connection string is required to connect to the Event Hubs service.
        eventhubs.consumerGroup (str): A consumer group is a view of an entire event hub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets.
        eventhubs.startingPosition (JSON str): The starting position for your Structured Streaming job. If a specific EventPosition is not set for a partition using startingPositions, then we use the EventPosition set in startingPosition. If nothing is set in either option, we will begin consuming from the end of the partition.
        eventhubs.endingPosition: (JSON str): The ending position of a batch query. This works the same as startingPosition.
        maxEventsPerTrigger (long): Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. 
    '''
    options: dict

    def __init__(self,options: dict) -> None:
        self.options = options

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        spark_libraries = Libraries()
        spark_libraries.add_maven_library(DEFAULT_PACKAGES["spark_azure_eventhub"])
        return spark_libraries
    
    @staticmethod
    def settings() -> dict:
        return {
            "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
            "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        }
    
    def pre_write_validation(self):
        return True
    
    def post_write_validation(self):
        return True

    def write_batch(self, df: DataFrame):
        '''
        Writes batch data to Event Hubs.
        '''
        try:
            return (
                df
                .write
                .format("eventhubs")
                .options(**self.options)
                .save()
            )

        except Py4JJavaError as e:
            logging.exception('error with spark write function', e.errmsg)
            raise e
        except Exception as e:
            logging.exception('error with spark write batch delta function', e.__traceback__)
            raise e
        
    def write_stream(self, df: DataFrame, options: dict, mode: str = "append") -> DataFrame:
        '''
        Writes steaming data to Event Hubs.
        '''
        try:
            return (df
                .writeStream
                .format("eventhubs")
                .options(**self.options)
                .start()
            )

        except Py4JJavaError as e:
            logging.exception('error with spark write function', e.errmsg)
            raise e
        except Exception as e:
            logging.exception('error with spark write stream delta function', e.__traceback__)
            raise e