import logging
import time
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
import requests
from pyspark.sql import SparkSession

from . import BaseISOSource
from ...._pipeline_utils.iso import PJM_PRICING_SCHEMA


class PJMDailyPricingISOSource(BaseISOSource):
    """
    The PJM Daily Pricing ISO Source is used to retrieve Real-Time and Day-Ahead hourly data from PJM API.
    Real-Time will return data for T - 3 to T days and Day-Ahead will return T - 3 to T + 1 days data.

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
        self.days: int = self.options.get("days", 3)

    def _fetch_paginated_data(
        self, url_suffix: str, start_date: str, end_date: str
    ) -> bytes:
        """
        Fetches data from the PJM API with pagination support.

        Args:
            url_suffix: String to be used as suffix to ISO URL.
            start_date: Start date for the data retrieval.
            end_date: End date for the data retrieval.

        Returns:
            Raw content of the data received.
        """
        headers = {"Ocp-Apim-Subscription-Key": self.api_key}
        items = []
        query = {
            "startRow": "1",
            "rowCount": "5",
            "datetime_beginning_ept": f"{start_date}to{end_date}",
        }
        query_s = "&".join(["=".join([k, v]) for k, v in query.items()])
        base_url = f"{self.iso_url}{url_suffix}?{query_s}"

        next_page = base_url

        logging.info(
            f"Requesting URL - {base_url}, start_date={start_date}, end_date={end_date}, load_type={self.load_type}"
        )

        while next_page:
            now = datetime.now()
            logging.info(f"Timestamp: {now}")
            response = requests.get(next_page, headers=headers)
            code = response.status_code

            if code != 200:
                raise requests.HTTPError(
                    f"Unable to access URL `{next_page}`."
                    f" Received status code {code} with message {response.content}"
                )

            data = response.json()
            print(data)
            # with open("File.json", "wb") as f:
            #     f.write(response.content)
            # exit()
            logging.info(f"Data for page {next_page}:")
            items.extend(data["items"])
            next_urls = list(filter(lambda item: item["rel"] == "next", data["links"]))
            next_page = next_urls[0]["href"] if next_urls else None
            time.sleep(10)

        return items

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the PJM API and parses the return.

        Returns:
            Raw form of data.
        """
        start_date = self.current_date - timedelta(self.days)
        start_date = start_date.replace(hour=0, minute=0)
        end_date = (start_date + timedelta(days=self.days)).replace(hour=23)
        start_date_str = start_date.strftime(self.query_datetime_format)
        end_date_str = end_date.strftime(self.query_datetime_format)

        if self.load_type == "day_ahead":
            url_suffix = "da_hrl_lmps"
        else:
            url_suffix = "rt_hrl_lmps"

        data = self._fetch_paginated_data(url_suffix, start_date_str, end_date_str)

        df = pd.DataFrame(data)
        logging.info(f"Data fetched successfully: {len(df)} rows")

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
                    "version_nbr": "VersionNbr",
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
                    "version_nbr": "VersionNbr",
                }
            )

        df = df[
            [
                "StartTime",
                "PnodeId",
                "PnodeName",
                "Voltage",
                "Equipment",
                "Type",
                "Zone",
                "SystemEnergyPrice",
                "TotalLmp",
                "CongestionPrice",
                "MarginalLossPrice",
                "VersionNbr",
            ]
        ]

        df = df.replace({np.nan: None, "": None})

        df["StartTime"] = pd.to_datetime(df["StartTime"])
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
