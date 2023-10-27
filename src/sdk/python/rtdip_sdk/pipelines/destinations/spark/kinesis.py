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
    """
    This Kinesis destination class is used to write batch or streaming data to Kinesis. Kinesis configurations need to be specified as options in a dictionary.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.destinations import SparkKinesisDestination

    kinesis_destination = SparkKinesisDestination(
        data=df,
        options={
            "endpointUrl": "https://kinesis.{REGION}.amazonaws.com",
            "awsAccessKey": "{YOUR-AWS-ACCESS-KEY}",
            "awsSecretKey": "{YOUR-AWS-SECRET-KEY}",
            "streamName": "{YOUR-STREAM-NAME}"
        },
        mode="update",
        trigger="10 seconds",
        query_name="KinesisDestination",
        query_wait_interval=None
    )

    kinesis_destination.write_stream()

    OR

    kinesis_destination.write_batch()
    ```

    Parameters:
        data (DataFrame): Dataframe to be written to Delta
        options (dict): A dictionary of Kinesis configurations (See Attributes table below). All Configuration options for Kinesis can be found [here.](https://github.com/qubole/kinesis-sql#kinesis-sink-configuration){ target="_blank" }
        mode (str): Method of writing to Kinesis - append, complete, update
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None

    Attributes:
        endpointUrl (str): Endpoint of the kinesis stream.
        awsAccessKey (str): AWS access key.
        awsSecretKey (str): AWS secret access key corresponding to the access key.
        streamName (List[str]): Name of the streams in Kinesis to write to.
    """

    data: DataFrame
    options: dict
    mode: str
    trigger: str
    query_name: str
    query_wait_interval: int

    def __init__(
        self,
        data: DataFrame,
        options: dict,
        mode: str = "update",
        trigger: str = "10 seconds",
        query_name="KinesisDestination",
        query_wait_interval: int = None,
    ) -> None:
        self.data = data
        self.options = options
        self.mode = mode
        self.trigger = trigger
        self.query_name = query_name
        self.query_wait_interval = query_wait_interval

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK_DATABRICKS
        """
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
        """
        Writes batch data to Kinesis.
        """
        try:
            return self.data.write.format("kinesis").options(**self.options).save()
        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self):
        """
        Writes steaming data to Kinesis.
        """
        try:
            TRIGGER_OPTION = (
                {"availableNow": True}
                if self.trigger == "availableNow"
                else {"processingTime": self.trigger}
            )
            query = (
                self.data.writeStream.trigger(**TRIGGER_OPTION)
                .format("kinesis")
                .outputMode(self.mode)
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
