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

from datetime import datetime
import logging
import pandas as pd
import numpy as np
import requests
from pyspark.sql import SparkSession
from datetime import timedelta
from io import BytesIO


from ...._pipeline_utils.iso import PJM_PRICING_SCHEMA

from . import BaseISOSource


class PJMDailyPricingISOSource(BaseISOSource):
    """
    The PJM Daily Pricing ISO Source is used to read Real-Time and Day-Ahead hourly data from PJM API. It supports both Real-Time Hourly LMPs and Day-Ahead Hourly LMPs data. Real-Time will return privous 1 day, Day-Ahead will return T + 1 days data.

    API:              <a href="https://api.pjm.com/api/v1/">  (must be a valid apy key from PJM)

    Real-Time doc:    <a href="https://dataminer2.pjm.com/feed/da_hrl_lmps/definition">

    Day-Ahead doc:    <a href="https://dataminer2.pjm.com/feed/rt_hrl_lmps/definition">
    
    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below)

    Attributes:
        api_key (str): Must be a valid key from PJM, see api url
        load_type (str): Must be one of `real_time` or `day_ahead`

    Please check the BaseISOSource for available methods.

    BaseISOSource:
        ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.base_iso
    """

    spark: SparkSession
    spark_schema = PJM_PRICING_SCHEMA
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
        
        load_key = "datetime_beginning_ept"

        feed = (
            "rt_hrl_lmps"
            if self.load_type != "day_ahead"
            else "da_hrl_lmps"
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
        start_date = self.current_date - timedelta(days=7)
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

        if self.load_type == "day_ahead":
            df = df.rename(
                columns={
                    "datetime_beginning_utc": "StartTime",
                    "pnode_id": "PnodeId",
                    "pnode_name": "PnodeName",
                    "voltage": "Voltage",
                    "equipment": "Equipment",
                    "type": "Type",
                    "zone": "Zone",
                    "system_energy_price_da": "SystemEnergyPrice",
                    "total_lmp_da": "TotalLmp",
                    "congestion_price_da": "CongestionPrice",
                    "marginal_loss_price_da": "MarginalLossPrice",
                    "version_nbr": "VersionNbr"
                    
                }
            )
        else:
            df = df.rename(
                columns={
                    "datetime_beginning_utc": "StartTime",
                    "pnode_id": "PnodeId",
                    "pnode_name": "PnodeName",
                    "voltage": "Voltage",
                    "equipment": "Equipment",
                    "type": "Type",
                    "zone": "Zone",
                    "system_energy_price_rt": "SystemEnergyPrice",
                    "total_lmp_rt": "TotalLmp",
                    "congestion_price_rt": "CongestionPrice",
                    "marginal_loss_price_rt": "MarginalLossPrice",
                    "version_nbr": "VersionNbr"
                    
                }
            )
                
        df = df[["StartTime", "PnodeId", "PnodeName", "Voltage", "Equipment", "Type", "Zone", "SystemEnergyPrice", "TotalLmp", "CongestionPrice", "MarginalLossPrice","VersionNbr"]]
        
        df = df.replace({np.nan: None, "": None})
    
        df["StartTime"] = pd.to_datetime(df["StartTime"], format="%m/%d/%Y %I:%M:%S %p")
        
    
        df = df.replace({np.nan: None, "": None})

        df.reset_index(inplace=True, drop=True)
        

        return df

    def _validate_options(self) -> bool:
        """
        Validates the following options:
            - `load_type` must be valid.

        Returns:
            True if all looks good otherwise raises Exception.
        """

        valid_load_types = ["real_time", "day_ahead"]

        if self.load_type not in valid_load_types:
            raise ValueError(
                f"Invalid load_type `{self.load_type}` given. Supported values are {valid_load_types}."
            )

        return True
