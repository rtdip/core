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
from datetime import datetime
from io import BytesIO

from ...._pipeline_utils.iso import PJM_SCHEMA
from . import BaseISOSource


class PJMDailyLoadISOSource(BaseISOSource):
    """
    The PJM Daily Load ISO Source is used to read daily load data from PJM API. It supports both Actual/historical and Forecast data.

    https://api.pjm.com/api/v1/
    https://dataminer2.pjm.com/feed/load_frcstd_hist/definition
    https://dataminer2.pjm.com/feed/load_frcstd_7_day/definition
    
    Actual data need to check
    Forecast data need to check

    Args:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations

    Attributes:
        load_type (str): Must be one of `actual` or `forecast`
        date (str): Must be in `YYYYMMDD` format.
        api_key (str): looks like we need an API Key

    """

    spark: SparkSession
    spark_schema = PJM_SCHEMA  #don't know this yet...update iso.py pipeline_utils
    options: dict
    iso_url: str = "https://api.pjm.com/api/v1/" # assuming we can pass load_type liek MISO actual/history or forecast
    query_datetime_format: str = "%Y%m%d"
    required_options = ["api_key"] # assume api_key is some KV pair & that we need to pass as a param....

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark = spark
        self.options = options
        self.load_type = self.options.get("load_type", "")
        self.date = self.options.get("date", "").strip()
        self.api_key = self.options.get("api_key", "").strip()

    def _fetch_from_url(self, url_suffix: str) -> bytes:
        """
        Gets data from external ISO API.

        Args:
            url_suffix: String to be used as suffix to iso url.

        Returns:
            Raw content of the data received.

        """
        url = f"{self.iso_url}{url_suffix}"
        headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        logging.info(f"Requesting URL - {url}")

        query = {
                "startRow": "1",
                'datetime_beginning_ept':"2023-06-24to2023-06-25",
                'format': 'csv',
                'download': 'true',
                'rowCount': '10'
            }
        query_s = '&'.join(['='.join([k,v]) for k, v in query.items()])
        new_url = f'{url}ops_sum_prev_period?{query_s}'
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

        # data = self._fetch_from_url("")
        # data = data.decode("utf-8")
        # json.dump(data, open("api_res.json","w"), indent=4)

        #df = pd.read_csv(self._fetch_from_url(f""))  # think its csv don't kno if we skip rows etc
        df = pd.read_csv(BytesIO(self._fetch_from_url(f"")))
        print(df)
        #df.show()

        # this is how didgital Glyde did it in Ingestion3 current version...might be useful ...bedtime
        # https://bitbucket.org/innowatts/ingestion3.0/src/master/source/ingestion3/connectors/pjm_iso_adapter/pjm_iso_adapter_actual_forecast_connector.py










      
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
