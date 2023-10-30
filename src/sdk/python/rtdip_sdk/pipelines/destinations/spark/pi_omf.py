#  Copyright 2022 RTDIP
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
import requests
import math
from typing import Literal
from requests.exceptions import HTTPError
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    to_json,
    struct,
    col,
    row_number,
    collect_list,
    floor,
    when,
    coalesce,
    lit,
    size,
)
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError
import gzip
import json

from .rest_api import SparkRestAPIDestination
from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class SparkPIOMFDestination(DestinationInterface):
    """
    The Spark PI OMF Destination is used to write data to a Rest API PI endpoint.
    !!! Note
        While it is possible to use the `write_batch` method, it is easy to overwhlem a Rest API with large volumes of data.
        Consider reducing data volumes when writing to a Rest API in Batch mode to prevent API errors including throtting.
    Args:
        data (DataFrame): Dataframe to be merged into a Delta Table
        options (dict): A dictionary of options for streaming writes
        url (str): The Rest API Url
        username (str): Username for your PI destination
        password (str): Password for your PI destination
        message_length (int): The number of {time,value} messages to be packed into each row
        batch_size (int): The number of DataFrame rows to be used in each Rest API call
        method (str): The method to be used when calling the Rest API. Allowed values are POST, PATCH and PUT
        parallelism (int): The number of concurrent calls to be made to the Rest API
        omf_version (str): The OMF version to use
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None
        compression (optional bool): If True, sends gzip compressed data in the API call
        timeout: (optional int): Time in seconds to wait for the type and container messages to be sent.
    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
    """

    data: DataFrame
    options: dict
    url: str
    username: str
    password: str
    message_length: int
    batch_size: int
    method: str
    parallelism: int
    omf_version: str
    trigger: str
    query_name: str
    query_wait_interval: int
    compression: bool
    timeout: int

    def __init__(
        self,
        data: DataFrame,
        options: dict,
        url: str,
        username: str,
        password: str,
        message_length: int,
        batch_size: int,
        parallelism: int = 8,
        omf_version: str = "1.1",
        trigger="1 minutes",
        query_name: str = "PIOMFRestAPIDestination",
        query_wait_interval: int = None,
        compression: bool = False,
        timeout: int = 30,
        create_type_message: bool = True,
    ) -> None:  # NOSONAR
        self.data = data
        self.options = options
        self.url = f"{url}/omf"
        self.username = username
        self.password = password
        self.message_length = message_length
        self.batch_size = batch_size
        self.parallelism = parallelism
        self.omf_version = omf_version
        self.trigger = trigger
        self.query_name = query_name
        self.query_wait_interval = query_wait_interval
        self.compression = compression
        self.timeout = timeout
        self.create_type_message = (
            self._send_type_message() if create_type_message == True else None
        )
        self.container_ids = []
        self.data_headers = {
            "messagetype": "data",
            "action": "create",
            "messageformat": "JSON",
            "omfversion": self.omf_version,
            "x-requested-with": "xmlhttprequest",
        }

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
        self._setup_omf_execute(json.dumps(json_message), "type")

    def _send_container_message(self, micro_batch_df: DataFrame):
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
        if len(json_message) > 0:
            self._setup_omf_execute(json.dumps(json_message), "container")

    def _setup_omf_execute(
        self, data: str, message_type: Literal["type", "container", "data"]
    ):
        headers = {
            "messagetype": message_type,
            "action": "create",
            "messageformat": "JSON",
            "omfversion": self.omf_version,
            "x-requested-with": "xmlhttprequest",
        }
        if self.compression:
            data = gzip.compress(bytes(data, "utf-8"))
            headers["compression"] = "gzip"
        response = requests.post(
            url=self.url,
            headers=headers,
            data=data,
            verify=False,
            timeout=self.timeout,
            auth=(self.username, self.password),
        )  # NOSONAR
        if response.status_code not in [200, 201, 202]:
            raise HTTPError(
                "Response status : {} .Response message : {}".format(
                    str(response.status_code), response.text
                )
            )  # NOSONAR

    def _pre_batch_records_for_api_call(self, micro_batch_df: DataFrame):
        micro_batch_df = (
            micro_batch_df.withColumn(
                "int_values",
                when(
                    col("ValueType") == "integer",
                    struct(
                        col("EventTime").alias("time"),
                        col("Value").cast("integer").alias("Value"),
                    ),
                ).otherwise(None),
            )
            .withColumn(
                "float_values",
                when(
                    col("ValueType") == "float",
                    struct(
                        col("EventTime").alias("time"),
                        col("Value").cast("float").alias("Value"),
                    ),
                ).otherwise(None),
            )
            .withColumn(
                "string_values",
                when(
                    col("ValueType") == "string",
                    struct(
                        col("EventTime").alias("time"),
                        col("Value").cast("string").alias("Value"),
                    ),
                ).otherwise(None),
            )
            .withColumnRenamed("TagName", "containerid")
            .withColumn(
                "row_number",
                row_number().over(Window().orderBy(col("containerid"))),
            )
            .withColumn(
                "batch_id", floor((col("row_number") / self.message_length) - 0.01)
            )
        )
        micro_batch_df = micro_batch_df.groupBy("batch_id", "containerid").agg(
            collect_list(col("int_values")).alias("int_values"),
            collect_list(col("float_values")).alias("float_values"),
            collect_list(col("string_values")).alias("string_values"),
        )

        micro_batch_df = (
            micro_batch_df.withColumn(
                "int_payload",
                when(size(col("int_values")) == 0, None).otherwise(
                    to_json(
                        struct(
                            col("containerid"),
                            col("int_values").alias("values"),
                        )
                    ),
                ),
            )
            .withColumn(
                "float_payload",
                when(size(col("float_values")) == 0, None).otherwise(
                    to_json(
                        struct(
                            col("containerid"),
                            col("float_values").alias("values"),
                        )
                    ),
                ),
            )
            .withColumn(
                "string_payload",
                when(size(col("string_values")) == 0, None).otherwise(
                    to_json(
                        struct(
                            col("containerid"),
                            col("string_values").alias("values"),
                        )
                    ),
                ),
            )
        )
        micro_batch_df = micro_batch_df.withColumn(
            "payload",
            coalesce("int_payload", "float_payload", "string_payload"),
        )
        return micro_batch_df.select("payload")

    def _group_rows(self, micro_batch_df: DataFrame):
        batch_count = math.ceil(micro_batch_df.count() / self.batch_size)
        micro_batch_df = micro_batch_df.withColumn(
            "row_number", row_number().over(Window().orderBy(lit("A")))
        ).withColumn("batch_id", col("row_number") % batch_count)
        return micro_batch_df.groupBy("batch_id").agg(
            collect_list("payload").cast("string").alias("payload")
        )

    def _api_micro_batch(self):
        self._send_container_message(self.data)
        pi_omf_df = self._pre_batch_records_for_api_call(self.data)
        pi_omf_df = self._group_rows(pi_omf_df)
        pi_omf_df.show(truncate=False)
        return SparkRestAPIDestination(
            data=pi_omf_df,
            options=self.options,
            url=self.url,
            headers=self.data_headers,
            batch_size=1,
            method="POST",
            auth=(self.username, self.password),
            compression=self.compression,
        )._api_micro_batch(micro_batch_df=pi_omf_df, _execute_transformation=False)

    def write_batch(self):
        """
        Writes batch data to a PI Rest API
        """
        try:
            self._api_micro_batch()
        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def write_stream(self):
        """
        Writes streaming data to a PI Rest API
        """
        try:
            TRIGGER_OPTION = (
                {"availableNow": True}
                if self.trigger == "availableNow"
                else {"processingTime": self.trigger}
            )
            query = (
                self.data.writeStream.trigger(**TRIGGER_OPTION)
                .foreachBatch(self._api_micro_batch)
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
