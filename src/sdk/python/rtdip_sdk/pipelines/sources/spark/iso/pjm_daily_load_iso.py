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
from datetime import timedelta
from io import BytesIO

import numpy as np
import pandas as pd
import requests
from pyspark.sql import SparkSession

from . import BaseISOSource
from ...._pipeline_utils.iso import PJM_SCHEMA


class PJMDailyLoadISOSource(BaseISOSource):
    """
    The PJM Daily Load ISO Source is used to read daily load data from PJM API.
    It supports both Actual and Forecast data. Actual will return 1 day, Forecast will return 7 days.

    To read more about the reports, visit the following URLs -
    <br>
    Actual doc:    [ops_sum_prev_period](https://dataminer2.pjm.com/feed/ops_sum_prev_period/definition)
    <br>
    Forecast doc:  [load_frcstd_7_day](https://dataminer2.pjm.com/feed/load_frcstd_7_day/definition)

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.sources import PJMDailyLoadISOSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    pjm_source = PJMDailyLoadISOSource(
        spark=spark,
        options={
            "api_key": "{api_key}",
            "load_type": "actual"
        }
    )

    pjm_source.read_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below)

    Attributes:
        api_key (str): Must be a valid key from PJM, see api url
        load_type (str): Must be one of `actual` or `forecast`

    Please check the BaseISOSource for available methods.

    BaseISOSource:
        ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.base_iso
    """

    spark: SparkSession
    spark_schema = PJM_SCHEMA
    options: dict
    iso_url: str = "https://api.pjm.com/api/v1/"
    query_datetime_format: str = "%Y-%m-%d %H:%M"
    required_options = ["api_key", "load_type"]
    default_query_timezone = "US/Eastern"

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark: SparkSession = spark
        self.options: dict = options
        self.load_type: str = self.options.get("load_type", "").strip()
        self.api_key: str = self.options.get("api_key", "").strip()
        self.days: int = self.options.get("days", 7)

    def _fetch_from_url(self, url_suffix: str, start_date: str, end_date: str) -> bytes:
        """
        Gets data from external ISO API.

        Args:
            url_suffix: String to be used as suffix to iso url.

        Returns:
            Raw content of the data received.
        """

        url = f"{self.iso_url}{url_suffix}"
        headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        logging.info(
            f"Requesting URL - {url}, start_date={start_date}, end_date={end_date}, load_type={self.load_type}"
        )
        load_key = (
            "datetime_beginning_ept"
            if self.load_type != "forecast"
            else "forecast_datetime_beginning_ept"
        )
        feed = (
            "ops_sum_prev_period"
            if self.load_type != "forecast"
            else "load_frcstd_7_day"
        )
        query = {
            "startRow": "1",
            load_key: f"{start_date}to{end_date}",
            "format": "csv",
            "download": "true",
        }
        query_s = "&".join(["=".join([k, v]) for k, v in query.items()])
        new_url = f"{url}{feed}?{query_s}"
        response = requests.get(new_url, headers=headers)
        code = response.status_code

        if code != 200:
            raise requests.HTTPError(
                f"Unable to access URL `{url}`."
                f" Received status code {code} with message {response.content}"
            )
        return response.content

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the PJM API and parses the return.

        Returns:
            Raw form of data.
        """
        start_date = self.current_date - timedelta(days=1)
        start_date = start_date.replace(hour=0, minute=0)
        end_date = (start_date + timedelta(days=self.days)).replace(hour=23)
        start_date_str = start_date.strftime(self.query_datetime_format)
        end_date_str = end_date.strftime(self.query_datetime_format)
        df = pd.read_csv(
            BytesIO(self._fetch_from_url("", start_date_str, end_date_str))
        )

        return df

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a new date time column and removes null values. Renames columns

        Args:
            df: Raw form of data received from the API.

        Returns:
            Data after basic transformations.

        """

        if self.load_type == "forecast":
            df = df.rename(
                columns={
                    "forecast_datetime_beginning_utc": "start_time",
                    "forecast_area": "zone",
                    "forecast_datetime_ending_utc": "end_time",
                    "forecast_load_mw": "load",
                }
            )
        else:
            df = df.rename(
                columns={
                    "datetime_beginning_utc": "start_time",
                    "area": "zone",
                    "datetime_ending_utc": "end_time",
                    "actual_load": "load",
                }
            )

        df = df[["start_time", "end_time", "zone", "load"]]
        df = df.replace({np.nan: None, "": None})

        date_cols = ["start_time", "end_time"]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], format="%m/%d/%Y %I:%M:%S %p")

        df["load"] = df["load"].astype(float)
        df = df.replace({np.nan: None, "": None})
        df.columns = list(map(lambda x: x.upper(), df.columns))

        rename_cols = {
            "START_TIME": "StartTime",
            "END_TIME": "EndTime",
            "ZONE": "Zone",
            "LOAD": "Load",
        }

        df = df.rename(columns=rename_cols)

        df.reset_index(inplace=True, drop=True)

        return df

    def _validate_options(self) -> bool:
        """
        Validates the following options:
            - `load_type` must be valid.

        Returns:
            True if all looks good otherwise raises Exception.
        """

        valid_load_types = ["actual", "forecast"]

        if self.load_type not in valid_load_types:
            raise ValueError(
                f"Invalid load_type `{self.load_type}` given. Supported values are {valid_load_types}."
            )

        return True
