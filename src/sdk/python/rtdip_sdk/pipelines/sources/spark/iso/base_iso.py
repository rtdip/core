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
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
import pytz
import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from requests import HTTPError

from ...interfaces import SourceInterface
from ...._pipeline_utils.models import Libraries, SystemType
from ....._sdk_utils.pandas import _prepare_pandas_to_convert_to_spark


class BaseISOSource(SourceInterface):
    """
    Base class for all the ISO Sources. It provides common functionality and helps in reducing the code redundancy.

    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations
    """

    spark: SparkSession
    options: dict
    iso_url: str = "https://"
    query_datetime_format: str = "%Y%m%d"
    required_options: list = []
    spark_schema = StructType([StructField("id", IntegerType(), True)])
    default_query_timezone: str = "UTC"

    def __init__(self, spark: SparkSession, options: dict) -> None:
        self.spark = spark
        self.options = options
        self.query_timezone = pytz.timezone(
            self.options.get("query_timezone", self.default_query_timezone)
        )
        self.current_date = datetime.now(timezone.utc).astimezone(self.query_timezone)

    def _fetch_from_url(self, url_suffix: str) -> bytes:
        """
        Gets data from external ISO API.

        Args:
            url_suffix: String to be used as suffix to iso url.

        Returns:
            Raw content of the data received.

        """
        url = f"{self.iso_url}{url_suffix}"
        logging.info(f"Requesting URL - {url}")

        response = requests.get(url)
        code = response.status_code

        if code != 200:
            raise HTTPError(
                f"Unable to access URL `{url}`."
                f" Received status code {code} with message {response.content}"
            )

        return response.content

    def _get_localized_datetime(self, datetime_str: str) -> datetime:
        """
        Converts string datetime into Python datetime object with configured format and timezone.
        Args:
            datetime_str: String to be converted into datetime.

        Returns: Timezone aware datetime object.

        """
        parsed_dt = datetime.strptime(datetime_str, self.query_datetime_format)
        parsed_dt = parsed_dt.replace(tzinfo=self.query_timezone)
        return parsed_dt

    def _pull_data(self) -> pd.DataFrame:
        """
        Hits the fetch_from_url method with certain parameters to get raw data from API.

        All the children ISO classes must override this method and call the fetch_url method
        in it.

        Returns:
             Raw DataFrame from API.
        """

        return pd.read_csv(BytesIO(self._fetch_from_url("")))

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Performs all the basic transformations to prepare data for further processing.
        All the children ISO classes must override this method.

        Args:
            df: Raw DataFrame, received from the API.

        Returns:
             Modified DataFrame, ready for basic use.

        """
        return df

    def _sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Another data transformation helper method to be called after prepare data.
        Used for advance data processing such as cleaning, filtering, restructuring.
        All the children ISO classes must override this method if there is any post-processing required.

        Args:
            df: Initial modified version of DataFrame, received after preparing the data.

        Returns:
             Final version of data after all the fixes and modifications.

        """
        return df

    def _get_data(self) -> pd.DataFrame:
        """
        Entrypoint method to return the final version of DataFrame.

        Returns:
            Modified form of data for specific use case.

        """
        df = self._pull_data()
        df = self._prepare_data(df)
        df = self._sanitize_data(df)

        # Reorder columns to keep the data consistent
        df = df[self.spark_schema.names]

        return df

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def _validate_options(self) -> bool:
        """
        Performs all the options checks. Raises exception in case of any invalid value.
        Returns:
             True if all checks are passed.

        """
        return True

    def pre_read_validation(self) -> bool:
        """
        Ensures all the required options are provided and performs other validations.
        Returns:
             True if all checks are passed.

        """
        for key in self.required_options:
            if key not in self.options:
                raise ValueError(f"Required option `{key}` is missing.")

        return self._validate_options()

    def post_read_validation(self) -> bool:
        return True

    def read_batch(self) -> DataFrame:
        """
        Spark entrypoint, It executes the entire process of pulling, transforming & fixing data.
        Returns:
             Final Spark DataFrame converted from Pandas DataFrame post-execution.

        """

        try:
            self.pre_read_validation()
            pdf = self._get_data()
            pdf = _prepare_pandas_to_convert_to_spark(pdf)

            # The below is to fix the compatibility issues between Pandas 2.0 and PySpark.
            pd.DataFrame.iteritems = pd.DataFrame.items
            df = self.spark.createDataFrame(data=pdf, schema=self.spark_schema)
            return df

        except Exception as e:
            logging.exception(str(e))
            raise e

    def read_stream(self) -> DataFrame:
        """
        By default, the streaming operation is not supported but child classes can override if ISO supports streaming.

        Returns:
             Final Spark DataFrame after all the processing.

        """

        raise NotImplementedError(
            f"{self.__class__.__name__} connector doesn't support stream operation."
        )
