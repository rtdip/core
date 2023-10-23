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
from datetime import datetime
import time
import requests
from requests.exceptions import HTTPError
from requests.adapters import HTTPAdapter
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    to_json,
    struct,
    col,
    row_number,
    collect_list,
    udf,
    floor,
    array,
)
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError
import gzip

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package

from pyspark.sql import SparkSession

import json

import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession.builder.appName("LocalModeExample").getOrCreate()


@udf("string")
def _omf_execute(data):
    # if self.compression:
    data = gzip.compress(bytes(data, "utf-8"))
    url = ""
    headers = {
        "messagetype": "data",
        "action": "create",
        "messageformat": "JSON",
        "omfversion": "1.1",
        "compression": "gzip",
        "x-requested-with": "xmlhttprequest",
    }
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=3)
    session.mount("http://", adapter)  # NOSONAR
    session.mount("https://", adapter)

    response = session.post(
        url=url,
        headers=headers,
        data=data,
        verify=False,
        timeout=30,
        auth=("", ""),
    )
    if response.status_code != 202:
        raise HTTPError(
            "Response status : {} .Response message : {}".format(
                str(response.status_code), response.text
            )
        )  # NOSONAR

    return str(response.status_code)


class SparkPIOMFDestination(DestinationInterface):
    """
    The Spark Rest API Destination is used to write data to a Rest API.

    The payload sent to the API is constructed by converting each row in the DataFrame to Json.

    !!! Note
        While it is possible to use the `write_batch` method, it is easy to overwhlem a Rest API with large volumes of data.
        Consider reducing data volumes when writing to a Rest API in Batch mode to prevent API errors including throtting.

    Args:
        data (DataFrame): Dataframe to be merged into a Delta Table
        options (dict): A dictionary of options for streaming writes
        url (str): The Rest API Url
        type_message (dict): OMF type message
        container_message (dict): OMF container message
        headers (dict): A dictionary of headers to be provided to the Rest API
        batch_size (int): The number of DataFrame rows to be used in each Rest API call
        method (str): The method to be used when calling the Rest API. Allowed values are POST, PATCH and PUT
        parallelism (int): The number of concurrent calls to be made to the Rest API
        omf_version (str): The OMF version to use
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None
        compression (optional bool): If True, sends gzip compressed data in the API call
        verify_ssl

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
    """

    data: DataFrame
    options: dict
    url: str
    username: str
    password: str
    batch_size: int
    method: str
    parallelism: int
    omf_version: str
    trigger: str
    query_name: str
    query_wait_interval: int
    compression: bool
    verify_ssl: bool
    timeout: int

    def __init__(
        self,
        data: DataFrame,
        options: dict,
        url: str,
        username: str,
        password: str,
        batch_size: int,
        parallelism: int = 8,
        omf_version: str = "1.1",
        trigger="1 minutes",
        query_name: str = "PIOMFRestAPIDestination",
        query_wait_interval: int = None,
        compression: bool = False,
        verify_ssl: bool = False,
        timeout: int = 30,
        create_type_message: bool = True,
    ) -> None:
        self.data = data
        self.options = options
        self.url = f"{url}/omf"
        self.username = username
        self.password = password
        self.batch_size = batch_size
        self.parallelism = parallelism
        self.omf_version = omf_version
        self.trigger = trigger
        self.query_name = query_name
        self.query_wait_interval = query_wait_interval
        self.compression = compression
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.create_type_message = (
            self._send_type_message() if create_type_message == True else None
        )
        self.container_ids = []

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_pypi_library(get_default_package("api_requests"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_write_validation(self):
        return True

    def post_write_validation(self):
        return True

    def _send_type_message(self):
        json_message = [
            {
                "id": "RTDIPstring",
                "version": "1.0.0.0",
                "type": "object",
                "classification": "dynamic",
                "properties": {
                    "time": {"type": "string", "format": "date-time", "isindex": True},
                    "Value": {"type": "string"},
                },
            },
            {
                "id": "RTDIPnumber",
                "version": "1.0.0.0",
                "type": "object",
                "classification": "dynamic",
                "properties": {
                    "time": {"type": "string", "format": "date-time", "isindex": True},
                    "Value": {"type": "number"},
                },
            },
        ]
        self._setup_omf_execute(json_message, "type")

    def _send_container_message(self, micro_batch_df):
        distinct_values_df = micro_batch_df.select("TagName", "ValueType").distinct()
        json_message = []
        for row in distinct_values_df.collect():
            if row["TagName"] not in self.container_ids:
                json_message.append(
                    {
                        "id": row["TagName"],
                        "typeid": "RTDIPnumber"
                        if row["ValueType"].lower() in ["integer", "float"]
                        else "RTDIPstring",
                    }
                )
                self.container_ids.append(row["TagName"])
        self._setup_omf_execute(json_message, "container")

    def _pre_batch_records_for_api_call(self, micro_batch_df: DataFrame):
        micro_batch_df = (
            micro_batch_df.withColumn(
                "values",
                struct(
                    col("EventTime").alias("time"),
                    col("Value").alias("Value"),
                ),
            )
            .withColumn(
                "row_number", row_number().over(Window().orderBy(col("TagName")))
            )
            .withColumn("batch_id", floor((col("row_number") / self.batch_size) - 0.01))
        )
        micro_batch_df = micro_batch_df.groupBy("batch_id", "TagName").agg(
            collect_list("values").alias("values")
        )
        micro_batch_df = micro_batch_df.withColumn(
            "payload",
            to_json(array(struct(col("TagName").alias("containerid"), col("values")))),
        )
        return micro_batch_df

    def _setup_omf_execute(self, data, message_type):
        headers = {
            "messagetype": message_type,
            "action": "create",
            "messageformat": "JSON",
            "omfversion": self.omf_version,
            "x-requested-with": "xmlhttprequest",
        }
        data = json.dumps(data)
        if self.compression:
            data = gzip.compress(bytes(data, "utf-8"))
            headers["compression"] = "gzip"

        response = requests.post(
            url=self.url,
            headers=headers,
            data=data,
            verify=self.verify_ssl,
            timeout=self.timeout,
            auth=(self.username, self.password),
        )
        if response.status_code not in [200, 201, 202]:
            raise HTTPError(
                "Response status : {} .Response message : {}".format(
                    str(response.status_code), response.text
                )
            )  # NOSONAR

    # def outer(self, data):
    #     @udf("string")
    #     def _omf_execute(data):
    #         if self.compression:
    #             data = gzip.compress(bytes(data, "utf-8"))
    #         url = self.url
    #         headers = {
    #             "messagetype": "data",
    #             "action": "create",
    #             "messageformat": "JSON",
    #             "omfversion": "1.1",
    #             "compression": "gzip",
    #             "x-requested-with": "xmlhttprequest",
    #         }
    #         session = requests.Session()
    #         adapter = HTTPAdapter(max_retries=3)
    #         session.mount("http://", adapter)  # NOSONAR
    #         session.mount("https://", adapter)

    #         response = session.post(
    #             url=url,
    #             headers=headers,
    #             data=data,
    #             verify=False,
    #             timeout=30,
    #             auth=(self.username, self.password),
    #         )
    #         if response.status_code != 202:
    #             raise HTTPError(
    #                 "Response status : {} .Response message : {}".format(
    #                     str(response.status_code), response.text
    #                 )
    #             )  # NOSONAR
    #         return str(response.status_code)

    #     data.withColumn(
    #         "rest_api_response_code",
    #         _omf_execute(
    #             data["payload"],
    #         ),
    #     ).collect()
    #     _omf_execute(data)

    def _api_micro_batch(self, micro_batch_df: DataFrame):  # NOSONAR
        micro_batch_df.persist()
        self._send_container_message(micro_batch_df)
        micro_batch_df = self._pre_batch_records_for_api_call(micro_batch_df)

        micro_batch_df = micro_batch_df.repartition(self.parallelism)

        micro_batch_df.withColumn(
            "rest_api_response_code",
            _omf_execute(
                micro_batch_df["payload"],
            ),
        ).collect()
        micro_batch_df.unpersist()

    def write_batch(self):
        """
        Writes batch data to a Rest API
        """
        try:
            # @udf("string")
            # def _omf_execute(data):
            #     if self.compression:
            #         data = gzip.compress(bytes(data, "utf-8"))
            #     url = self.url
            #     headers = {
            #         "messagetype": "data",
            #         "action": "create",
            #         "messageformat": "JSON",
            #         "omfversion": "1.1",
            #         "compression": "gzip",
            #         "x-requested-with": "xmlhttprequest",
            #     }
            #     session = requests.Session()
            #     adapter = HTTPAdapter(max_retries=3)
            #     session.mount("http://", adapter)  # NOSONAR
            #     session.mount("https://", adapter)

            #     response = session.post(
            #         url=url,
            #         headers=headers,
            #         data=data,
            #         verify=False,
            #         timeout=30,
            #         auth=(self.username, self.password),
            #     )
            #     if not response.status_code == 202:
            #         raise HTTPError(
            #             "Response status : {} .Response message : {}".format(
            #                 str(response.status_code), response.text
            #             )
            #         )  # NOSONAR

            #     return str(response.status_code)

            # self.data.persist()
            # self._send_container_message(self.data)
            # micro_batch_df = self._pre_batch_records_for_api_call(self.data)

            # micro_batch_df = micro_batch_df.repartition(self.parallelism)
            # micro_batch_df.withColumn(
            #     "rest_api_response_code",
            #     _omf_execute(
            #         micro_batch_df["payload"],
            #     ),
            # ).collect()
            # self.outer(micro_batch_df)
            # micro_batch_df.unpersist()
            return self._api_micro_batch(self.data)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self):
        """
        Writes streaming data to a Rest API
        """
        try:
            TRIGGER_OPTION = (
                {"availableNow": True}
                if self.trigger == "availableNow"
                else {"processingTime": self.trigger}
            )
            query = (
                self.data.writeStream.trigger(**TRIGGER_OPTION)
                .foreachBatch(self.write_batch)
                .queryName(self.query_name)
                .outputMode("update")
                .options(**self.options)
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


if __name__ == "__main__":
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        TimestampType,
        IntegerType,
        FloatType,
    )
    from pyspark.sql import SparkSession

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", StringType(), True),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), True),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "TagName": "Sample.Script.RTDIP7",
            "EventTime": "2023-10-01T15:44:56Z",
            # datetime.fromisoformat("2023-09-30T06:54:00+00:00"),
            "Status": "Good",
            "Value": "7.3",
            "ValueType": "string",
            "ChangeType": "insert",
        },
        {
            "TagName": "Sample.Script.RTDIP7",
            "EventTime": "2023-09-30T06:56:00+00:00",
            "Status": "Good",
            "Value": "6.2",
            "ValueType": "str",
            "ChangeType": "insert",
        },
        {
            "TagName": "RTDIP8",
            "EventTime": "2023-09-30T07:58:01+00:00",
            "Status": "Good",
            "Value": "5.0",
            "ValueType": "str",
            "ChangeType": "insert",
        },
    ]

    sample_df: DataFrame = spark.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    SparkPIOMFDestination(
        data=sample_df,
        options={},
        url="",
        username="",
        password="",
        batch_size=1,
        compression=True,
        create_type_message=True,
    ).write_batch()
