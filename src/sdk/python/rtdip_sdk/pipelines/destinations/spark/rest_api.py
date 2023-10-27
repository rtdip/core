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
import math
import requests
from requests.adapters import HTTPAdapter
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    to_json,
    struct,
    col,
    row_number,
    concat_ws,
    collect_list,
    lit,
    udf,
)
from pyspark.sql.window import Window
from py4j.protocol import Py4JJavaError

from ..interfaces import DestinationInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class SparkRestAPIDestination(DestinationInterface):
    """
    The Spark Rest API Destination is used to write data to a Rest API.

    The payload sent to the API is constructed by converting each row in the DataFrame to Json.

    !!! Note
        While it is possible to use the `write_batch` method, it is easy to overwhlem a Rest API with large volumes of data.
        Consider reducing data volumes when writing to a Rest API in Batch mode to prevent API errors including throtting.

    Example
    --------
    ```python
    #Rest API Destination for Streaming Queries

    from rtdip_sdk.pipelines.destinations import SparkRestAPIDestination

    rest_api_destination = SparkRestAPIDestination(
        data=df,
        options={
            "checkpointLocation": "{/CHECKPOINT-LOCATION/}"
        },
        url="{REST-API-URL}",
        headers = {
            'Authorization': 'Bearer {}'.format("{TOKEN}")
        },
        batch_size=100,
        method="POST",
        parallelism=8,
        trigger="1 minute",
        query_name="DeltaRestAPIDestination",
        query_wait_interval=None
    )

    rest_api_destination.write_stream()
    ```
    ```python
    #Rest API Destination for Batch Queries

    from rtdip_sdk.pipelines.destinations import SparkRestAPIDestination

    rest_api_destination = SparkRestAPIDestination(
        data=df,
        options={},
        url="{REST-API-URL}",
        headers = {
            'Authorization': 'Bearer {}'.format("{TOKEN}")
        },
        batch_size=10,
        method="POST",
        parallelism=4,
        trigger="1 minute",
        query_name="DeltaRestAPIDestination",
        query_wait_interval=None
    )

    rest_api_destination.write_stream()
    ```

    Parameters:
        data (DataFrame): Dataframe to be merged into a Delta Table
        options (dict): A dictionary of options for streaming writes
        url (str): The Rest API Url
        headers (dict): A dictionary of headers to be provided to the Rest API
        batch_size (int): The number of DataFrame rows to be used in each Rest API call
        method (str): The method to be used when calling the Rest API. Allowed values are POST, PATCH and PUT
        parallelism (int): The number of concurrent calls to be made to the Rest API
        trigger (optional str): Frequency of the write operation. Specify "availableNow" to execute a trigger once, otherwise specify a time period such as "30 seconds", "5 minutes". Set to "0 seconds" if you do not want to use a trigger. (stream) Default is 10 seconds
        query_name (str): Unique name for the query in associated SparkSession
        query_wait_interval (optional int): If set, waits for the streaming query to complete before returning. (stream) Default is None

    Attributes:
        checkpointLocation (str): Path to checkpoint files. (Streaming)
    """

    data: DataFrame
    options: dict
    url: str
    headers: dict
    batch_size: int
    method: str
    parallelism: int
    trigger: str
    query_name: str
    query_wait_interval: int

    def __init__(
        self,
        data: DataFrame,
        options: dict,
        url: str,
        headers: dict,
        batch_size: int,
        method: str = "POST",
        parallelism: int = 8,
        trigger="1 minutes",
        query_name: str = "DeltaRestAPIDestination",
        query_wait_interval: int = None,
    ) -> None:
        self.data = data
        self.options = options
        self.url = url
        self.headers = headers
        self.batch_size = batch_size
        self.method = method
        self.parallelism = parallelism
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

    def _pre_batch_records_for_api_call(self, micro_batch_df: DataFrame):
        batch_count = math.ceil(micro_batch_df.count() / self.batch_size)
        micro_batch_df = (
            micro_batch_df.withColumn("content", to_json(struct(col("*"))))
            .withColumn("row_number", row_number().over(Window().orderBy(lit("A"))))
            .withColumn("batch_id", col("row_number") % batch_count)
        )
        return micro_batch_df.groupBy("batch_id").agg(
            concat_ws(",|", collect_list("content")).alias("payload")
        )

    def _api_micro_batch(self, micro_batch_df: DataFrame, epoch_id=None):  # NOSONAR
        url = self.url
        method = self.method
        headers = self.headers

        @udf("string")
        def _rest_api_execute(data):
            session = requests.Session()
            adapter = HTTPAdapter(max_retries=3)
            session.mount("http://", adapter)  # NOSONAR
            session.mount("https://", adapter)

            if method == "POST":
                response = session.post(url, headers=headers, data=data, verify=False)
            elif method == "PATCH":
                response = session.patch(url, headers=headers, data=data, verify=False)
            elif method == "PUT":
                response = session.put(url, headers=headers, data=data, verify=False)
            else:
                raise Exception("Method {} is not supported".format(method))  # NOSONAR

            if not (response.status_code == 200 or response.status_code == 201):
                raise Exception(
                    "Response status : {} .Response message : {}".format(
                        str(response.status_code), response.text
                    )
                )  # NOSONAR

            return str(response.status_code)

        micro_batch_df.persist()
        micro_batch_df = self._pre_batch_records_for_api_call(micro_batch_df)

        micro_batch_df = micro_batch_df.repartition(self.parallelism)

        (
            micro_batch_df.withColumn(
                "rest_api_response_code", _rest_api_execute(micro_batch_df["payload"])
            ).collect()
        )
        micro_batch_df.unpersist()

    def write_batch(self):
        """
        Writes batch data to a Rest API
        """
        try:
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
