# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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

class SparkKinesisDestination(DestinationInterface):
    '''
    This Kinesis destination class is used to write batch or streaming data to Kinesis. Kinesis configurations need to be specified as options in a dictionary.
    Args:
        data (DataFrame): Dataframe to be written to Delta
        options (dict): A dictionary of Kinesis configurations (See Attributes table below). All Configuration options for Kinesis can be found [here.](https://github.com/qubole/kinesis-sql#kinesis-sink-configuration){ target="_blank" }
        mode (str): Method of writing to Kinesis - append, complete, update
        trigger (str): Frequency of the write operation
        
    Attributes:
        endpointUrl (str): Endpoint of the kinesis stream.
        awsAccessKey (str): AWS access key.
        awsSecretKey (str): AWS secret access key corresponding to the access key.
        streamName (List[str]): Name of the streams in Kinesis to write to.
    '''
    options: dict
    mode: str
    trigger: str

    def __init__(self, data: DataFrame, options: dict, mode:str = "update", trigger:str= "10 seconds") -> None:
        self.data = data
        self.options = options
        self.mode = mode
        self.trigger = trigger

    @staticmethod
    def system_type():
        '''
        Attributes:
            SystemType (Environment): Requires PYSPARK_DATABRICKS
        '''
        return SystemType.PYSPARK_DATABRICKS

    @staticmethod
    def libraries():
        spark_libraries = Libraries()
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
        Writes batch data to Kinesis.
        '''
        try:
            return (
                self.data
                .write
                .format("kinesis")
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
        Writes steaming data to Kinesis.
        '''
        try:
            query = (
                self.data
                .writeStream
                .trigger(processingTime=self.trigger)
                .format("kinesis")
                .outputMode(self.mode)
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