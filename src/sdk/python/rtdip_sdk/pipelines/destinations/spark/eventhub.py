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

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package
class SparkEventhubDestination(DestinationInterface):
    '''
    This Spark destination class is used to write batch or streaming data to Eventhubs. Eventhub configurations need to be specified as options in a dictionary.
    Additionally, there are more optional configurations which can be found [here.](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration){ target="_blank" }
    If using startingPosition or endingPosition make sure to check out **Event Position** section for more details and examples.

    Args:
        data (DataFrame): Dataframe to be written to Eventhub
        options (dict): A dictionary of Eventhub configurations (See Attributes table below). All Configuration options for Eventhubs can be found [here.](https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md#event-hubs-configuration){ target="_blank" }

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
        eventhubs.connectionString (str):  Eventhubs connection string is required to connect to the Eventhubs service. (Streaming and Batch)
        eventhubs.consumerGroup (str): A consumer group is a view of an entire eventhub. Consumer groups enable multiple consuming applications to each have a separate view of the event stream, and to read the stream independently at their own pace and with their own offsets. (Streaming and Batch)
        eventhubs.startingPosition (JSON str): The starting position for your Structured Streaming job. If a specific EventPosition is not set for a partition using startingPositions, then we use the EventPosition set in startingPosition. If nothing is set in either option, we will begin consuming from the end of the partition. (Streaming and Batch)
        eventhubs.endingPosition: (JSON str): The ending position of a batch query. This works the same as startingPosition. (Batch)
        maxEventsPerTrigger (long): Rate limit on maximum number of events processed per trigger interval. The specified total number of events will be proportionally split across partitions of different volume. (Stream)
    '''
    data: DataFrame
    options: dict

    def __init__(self, data: DataFrame, options: dict) -> None:
        self.data = data
        self.options = options

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
        spark_libraries.add_maven_library(get_default_package("spark_azure_eventhub"))
        return spark_libraries
    
    @staticmethod
    def settings() -> dict:
        return {}
    
    def pre_write_validation(self):
        return True
    
    def post_write_validation(self):
        return True

    def write_batch(self):
        '''
        Writes batch data to Eventhubs.
        '''
        try:
            return (
                self.data
                .write
                .format("eventhubs")
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
        '''
        Writes steaming data to Eventhubs.
        '''
        try:
            query = (
                self.data
                .writeStream
                .format("eventhubs")
                .options(**self.options)
                .start()
            )
            while query.isActive:
                if query.lastProgress:
                    logging.info(query.lastProgress)
                time.sleep(10)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e