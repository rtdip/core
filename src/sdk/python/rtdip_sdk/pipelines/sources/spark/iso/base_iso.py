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
from abc import abstractmethod
import pandas as pd
from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession
import requests
from datetime import datetime

from ...interfaces import SourceInterface
from ...._pipeline_utils.models import Libraries, SystemType

logging.getLogger().setLevel("INFO")

class BaseISOSource(SourceInterface):

    spark: SparkSession
    options: dict
    iso_url: str = ""
    query_datetime_format: str = "%Y%m%d"
    result_schema: list[str] = []
    requires_options: list[str] = []

    def __init__(self, spark: SparkSession, options: dict) -> None:

        self.spark = spark
        self.options = options
        self.current_date = datetime.utcnow()

    def fetch_from_url(self, url_suffix: str) -> bytes:

        url = f"{self.iso_url}{url_suffix}"
        logging.info(f"Requesting URL - {url}")

        response = requests.get(url)
        code = response.status_code

        if code != 200:
            raise Exception(f"Unable to access URL `{url}`."
                            f" Received status code {code} with message {response.content}")

        return response.content

    @abstractmethod
    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    @abstractmethod
    def sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

    @abstractmethod
    def pull_data(self) -> pd.DataFrame:
        return pd.DataFrame([])

    def get_data(self):

        # Fetch original data in DataFrame
        df = self.pull_data()

        # Transform data for readability and use
        df = self.prepare_data(df)

        # Perform sanitization.
        df = self.sanitize_data(df)

        # Reorder columns to keep the data consistent
        df = df[self.result_schema]

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

    @abstractmethod
    def validate_options(self) -> bool:
        return True

    def pre_read_validation(self):

        for key in self.requires_options:
            if key not in self.options:
                logging.error(f"Required option `{key}` is missing.")
                return False

        return self.validate_options()

    def post_read_validation(self):
        return True

    def read_batch(self):
        try:

            if self.pre_read_validation() is False:
                raise Exception("Initial checks failed, please check the logs for more info.")

            pdf = self.get_data()

            return self.spark.createDataFrame(pdf)

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e

    def read_stream(self) -> DataFrame:
        raise NotImplementedError(f"{self.__class__.__name__} connector currently doesn't support stream operation.")
