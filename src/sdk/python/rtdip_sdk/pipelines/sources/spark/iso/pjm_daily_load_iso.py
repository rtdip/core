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

import json
import logging
import pandas as pd
import requests
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from io import BytesIO

from ...._pipeline_utils.iso import PJM_SCHEMA
from . import BaseISOSource


class PJMDailyLoadISOSource(BaseISOSource):
    """
    The PJM Daily Load ISO Source is used to read daily load data from PJM API. It supports both Actual/historical and Forecast data.

    api        https://api.pjm.com/api/v1/
    actual     https://dataminer2.pjm.com/feed/ops_sum_prev_period/definition
    forecast   https://dataminer2.pjm.com/feed/load_frcstd_7_day/definition
    
    Args:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations

    Attributes:
        feed (str): a string of a valid service of the  api provided in iso_runner.py
        api_key (str): a string of a valid api_key provided in iso_runner.py
    """

    spark: SparkSession
    spark_schema = PJM_SCHEMA  #don't know this yet...update iso.py pipeline_utils
    options: dict
    iso_url: str = "https://api.pjm.com/api/v1/" 
    query_datetime_format: str = "%Y-%m-%d"
    required_options = ["feed","api_key"] # set in iso_runner.py


    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark = spark
        self.options = options
        self.feed = self.options.get("feed", "").strip()
        self.api_key = self.options.get("api_key", "").strip()
        self.days = self.options.get("days", 7)


    def _fetch_from_url(self, url_suffix: str, start_date: str, end_date: str) -> bytes:
        """
        Gets data from external ISO API.
        # this is how didgital Glyde did it in Ingestion3 current version...might be useful ...bedtime
        # https://bitbucket.org/innowatts/ingestion3.0/src/master/source/ingestion3/connectors/pjm_iso_adapter/pjm_iso_adapter_actual_forecast_connector.py

        Args:
            url_suffix: String to be used as suffix to iso url.

        Returns:
            Raw content of the data received.
        """

        url = f"{self.iso_url}{url_suffix}"
        feed = self.feed
        headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        logging.info(f"Requesting URL - {url}, start_date={start_date}, end_date={end_date}, feed={feed}")
        load_key = 'datetime_beginning_ept' if feed!='load_frcstd_7_day' else 'forecast_datetime_beginning_ept'
        query = {
                "startRow": "1",
                #'datetime_beginning_ept': f"{self.start_date}to{self.end_date}",
                load_key: f"{start_date}to{end_date}",
                'format': 'csv',
                'download': 'true'
            }
        query_s = '&'.join(['='.join([k,v]) for k, v in query.items()])
        new_url = f'{url}{feed}?{query_s}'
        response = requests.get(new_url, headers = headers)
        code = response.status_code

        if code != 200:
            raise requests.HTTPError(f"Unable to access URL `{url}`."
                            f" Received status code {code} with message {response.content}")
        return response.content
    

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the PJM API and parses the return.

        Returns:
            Raw form of data.
        """
        # # debug stuff
        # data = self._fetch_from_url("")
        # data = data.decode("utf-8")
        # json.dump(data, open("api_res.json","w"), indent=4)
        start_date = datetime.utcnow() - timedelta(days=1)
        end_date = start_date+ timedelta(days=self.days)
        start_date_str = start_date.strftime(self.query_datetime_format)
        end_date_str = end_date.strftime(self.query_datetime_format)
        df = pd.read_csv(BytesIO(self._fetch_from_url("", start_date_str, end_date_str)))
        print(df)
        exit(0)
        return df
       

     










      
    # def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Creates a new `date_time` column and removes null values.

    #     Args:
    #         df: Raw form of data received from the API.

    #     Returns:
    #         Data after basic transformations.

    #     """

    #     df.drop(df.index[(df['HourEnding'] == 'HourEnding') | df['PJM MTLF (MWh)'].isna()], inplace=True)
    #     df.rename(columns={'Market Day': 'date'}, inplace=True)

    #     df['date_time'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['HourEnding'].astype(int) - 1, 'h')
    #     df.drop(['HourEnding', 'date'], axis=1, inplace=True)

    #     data_cols = df.columns[df.columns != 'date_time']
    #     df[data_cols] = df[data_cols].astype(float)

    #     df.reset_index(inplace=True, drop=True)

    #     return df



    # def _sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:
    #     """
    #     Filter outs Actual or Forecast data based on `load_type`.
    #     Args:
    #         df: Data received after preparation.

    #     Returns:
    #         Final data either containing Actual or Forecast values.

    #     """

    #     skip_col_suffix = ""

    #     if self.load_type == "actual":
    #         skip_col_suffix = 'MTLF (MWh)'

    #     elif self.load_type == "forecast":
    #         skip_col_suffix = 'ActualLoad (MWh)'

    #     df = df[[x for x in df.columns if not x.endswith(skip_col_suffix)]]
    #     df = df.dropna()
    #     df.columns = [str(x.split(' ')[0]).upper() for x in df.columns]

    #     return df
    

    # def _validate_options(self) -> bool:
    #     """
    #     Validates the following options:
    #         - `date` must be in the correct format.
    #         - `load_type` must be valid.

    #     Returns:
    #         True if all looks good otherwise raises Exception.

    #     """

    #     try:
    #         datetime.strptime(self.date, self.query_datetime_format)
    #     except ValueError:
    #         raise ValueError("Unable to parse Date. Please specify in YYYYMMDD format.")

    #     valid_load_types = ["actual", "forecast"]

    #     if self.load_type not in valid_load_types:
    #         raise ValueError(f"Invalid load_type `{self.load_type}` given. Supported values are {valid_load_types}.")

    #     return True
