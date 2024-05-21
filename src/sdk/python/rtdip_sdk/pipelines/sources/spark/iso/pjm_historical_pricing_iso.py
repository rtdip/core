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

from datetime import datetime, timezone
import logging
import time
import pandas as pd
import numpy as np
import requests
from pyspark.sql import SparkSession
from datetime import timedelta
from io import BytesIO
from .pjm_daily_pricing_iso import PJMDailyPricingISOSource
from ...._pipeline_utils.iso import PJM_PRICING_SCHEMA
from . import BaseISOSource


class PJMHistoricalPricingISOSource(PJMDailyPricingISOSource):
    """
    The PJM Historical Pricing ISO Source is used to retrieve historical Real-Time and Day-Ahead hourly data from the PJM API.

    API:             <a href="https://api.pjm.com/api/v1/">https://api.pjm.com/api/v1/</a>  (must be a valid apy key from PJM)

    Real-Time doc:    <a href="https://dataminer2.pjm.com/feed/rt_hrl_lmps/definition">https://dataminer2.pjm.com/feed/rt_hrl_lmps/definition</a>

    Day-Ahead doc:    <a href="https://dataminer2.pjm.com/feed/da_hrl_lmps/definition">https://dataminer2.pjm.com/feed/da_hrl_lmps/definition</a>

    The PJM Historical Pricing ISO Source accesses the same PJM endpoints as the daily pricing source but is tailored for retrieving data within a specified historical range defined by the `start_date` and `end_date` attributes.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.sources import PJMHistoricalPricingISOSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    pjm_source = PJMHistoricalPricingISOSource(
        spark=spark,
        options={
            "api_key": "{api_key}",
            "start_date": "2023-05-10",
            "end_date": "2023-05-20",
        }
    )

    pjm_source.read_batch()
    ```

    Parameters:
        spark (SparkSession): The Spark Session instance.
        options (dict): A dictionary of ISO Source specific configurations.

    Attributes:
        api_key (str): A valid key from PJM required for authentication.
        load_type (str): The type of data to retrieve, either `real_time` or `day_ahead`.
        start_date (str): Must be in `YYYY-MM-DD` format.
        end_date (str): Must be in `YYYY-MM-DD` format.

    Please refer to the BaseISOSource for available methods and further details.

    BaseISOSource: ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.base_iso"""

    spark: SparkSession
    options: dict
    required_options = ["api_key", "load_type", "start_date", "end_date"]

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark: SparkSession = spark
        self.options: dict = options
        self.start_date: str = self.options.get("start_date", "")
        self.end_date: str = self.options.get("end_date", "")
        self.user_datetime_format = "%Y-%m-%d"

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls historical pricing data from the PJM API within the specified date range.

        Returns:
            pd.DataFrame: A DataFrame containing the raw historical pricing data retrieved from the PJM API.
        """

        logging.info(
            f"Historical data requested from {self.start_date} to {self.end_date}"
        )

        start_date_str = datetime.strptime(
            self.start_date, self.user_datetime_format
        ).replace(hour=0, minute=0)
        end_date_str = datetime.strptime(
            self.end_date, self.user_datetime_format
        ).replace(hour=23)

        if self.load_type == "day_ahead":
            url_suffix = "da_hrl_lmps"
        else:
            url_suffix = "rt_hrl_lmps"

        data = self._fetch_paginated_data(url_suffix, start_date_str, end_date_str)

        df = pd.DataFrame(data)
        logging.info(f"Data fetched successfully: {len(df)} rows")

        return df

    def _validate_options(self) -> bool:
        """
        Validates all parameters including the following examples:
            - `start_date` & `end_data` must be in the correct format.
            - `start_date` must be behind `end_data`.
            - `start_date` must not be in the future (UTC).

        Returns:
            True if all looks good otherwise raises Exception.

        """
        super()._validate_options()
        try:
            start_date = datetime.strptime(self.start_date, self.user_datetime_format)
        except ValueError:
            raise ValueError(
                f"Unable to parse Start date. Please specify in {self.user_datetime_format} format."
            )

        try:
            end_date = datetime.strptime(self.end_date, self.user_datetime_format)
        except ValueError:
            raise ValueError(
                f"Unable to parse End date. Please specify in {self.user_datetime_format} format."
            )

        if start_date > datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            days=1
        ):
            raise ValueError("Start date can't be in future.")

        if start_date > end_date:
            raise ValueError("Start date can't be ahead of End date.")

        if end_date > datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(
            days=1
        ):
            raise ValueError("End date can't be in future.")

        return True
