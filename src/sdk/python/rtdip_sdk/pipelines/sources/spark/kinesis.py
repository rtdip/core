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
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import KINESIS_SCHEMA


class SparkKinesisSource(SourceInterface):
    """
    The Spark Kinesis Source is used to read data from Kinesis in a Databricks environment.
    Structured streaming from Kinesis is **not** supported in open source Spark.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.sources import SparkKinesisSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    kinesis_source = SparkKinesisSource(
        spark=spark,
        options={
            "awsAccessKey": "{AWS-ACCESS-KEY}",
            "awsSecretKey": "{AWS-SECRET-KEY}",
            "streamName": "{STREAM-NAME}",
            "region": "{REGION}",
            "endpoint": "https://kinesis.{REGION}.amazonaws.com",
            "initialPosition": "earliest"
        }
    )

    kinesis_source.read_stream()

    OR

    kinesis_source.read_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session required to read data from Kinesis
        options (dict): Options that can be specified for a Kinesis read operation (See Attributes table below). Further information on the options is available [here](https://docs.databricks.com/structured-streaming/kinesis.html#configuration){ target="_blank" }

    Attributes:
        awsAccessKey (str): AWS access key.
        awsSecretKey (str): AWS secret access key corresponding to the access key.
        streamName (List[str]): The stream names to subscribe to.
        region (str): The region the streams are defined in.
        endpoint (str): The regional endpoint for Kinesis Data Streams.
        initialPosition (str): The point to start reading from; earliest, latest, or at_timestamp.
    """

    spark: SparkSession
    options: dict

    def __init__(self, spark: SparkSession, options: dict) -> None:
        self.spark = spark
        self.options = options
        self.schema = KINESIS_SCHEMA

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK_DATABRICKS
        """
        return SystemType.PYSPARK_DATABRICKS

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_read_validation(self):
        return True

    def post_read_validation(self, df: DataFrame) -> bool:
        assert df.schema == self.schema
        return True

    def read_batch(self):
        """
        Raises:
            NotImplementedError: Kinesis only supports streaming reads. To perform a batch read, use the read_stream method of this component and specify the Trigger on the write_stream to be `availableNow=True` to perform batch-like reads of cloud storage files.
        """
        raise NotImplementedError(
            "Kinesis only supports streaming reads. To perform a batch read, use the read_stream method and specify Trigger on the write_stream as `availableNow=True`"
        )

    def read_stream(self) -> DataFrame:
        """
        Reads streaming data from Kinesis. All of the data in the table is processed as well as any new data that arrives after the stream started.
        """
        try:
            return (
                self.spark.readStream.format("kinesis").options(**self.options).load()
            )
        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
