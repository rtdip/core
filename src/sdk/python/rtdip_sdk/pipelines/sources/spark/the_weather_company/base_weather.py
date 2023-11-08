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
import requests
from pyspark.sql import SparkSession
from requests import HTTPError
from ..iso import BaseISOSource


class SparkWeatherCompanyBaseWeatherSource(BaseISOSource):
    """
    Base class for all the Weather related sources. Provides common functionality.

    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of Weather Source specific configurations.

    """

    spark: SparkSession
    options: dict
    weather_url: str = "https://"
    api_params: dict = {}

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super(SparkWeatherCompanyBaseWeatherSource, self).__init__(spark, options)
        self.spark = spark
        self.options = options

    def _get_api_params(self) -> dict:
        return self.api_params

    def _fetch_weather_from_url(self, url_suffix: str, params: dict) -> bytes:
        """
        Gets data from external Weather Forecast API.

        Args:
            url_suffix: String to be used as suffix to weather url.

        Returns:
            Raw content of the data received.

        """
        url = f"{self.weather_url}{url_suffix}"
        logging.info(f"Requesting URL - `{url}` with params - {params}")

        response = requests.get(url, params)
        code = response.status_code

        if code != 200:
            raise HTTPError(
                f"Unable to access URL `{url}`."
                f" Received status code {code} with message {response.content}"
            )
        return response.content

    def _fetch_from_url(self, url_suffix: str) -> bytes:
        return self._fetch_weather_from_url(url_suffix, self._get_api_params())
