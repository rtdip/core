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
from _datetime import datetime
from datetime import timedelta

import pandas as pd
from pyspark.sql import SparkSession

from .caiso_daily_load_iso import CAISODailyLoadISOSource
from ...._pipeline_utils.iso import CAISO_SCHEMA


class CAISOHistoricalLoadISOSource(CAISODailyLoadISOSource):
    """
    The CAISO Daily Load ISO Source is used to read daily load data from CAISO API. It supports both Actual and Forecast data.

    API: <a href="http://oasis.caiso.com/oasisapi">http://oasis.caiso.com/oasisapi</a>


    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below)

    Attributes:
        load_types (list): Must be a subset of ['Demand Forecast 7-Day Ahead', 'Demand Forecast 2-Day Ahead',
         'Demand Forecast Day Ahead', 'RTM 15Min Load Forecast', 'RTM 5Min Load Forecast',
          'Total Actual Hourly Integrated Load']. Default to - ["Total Actual Hourly Integrated Load"]
        start_date (str): Must be in `YYYY-MM-DD` format.
        end_date (str): Must be in `YYYY-MM-DD` format.

    Please check the BaseISOSource for available methods.

    BaseISOSource:
        ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.base_iso
    """

    spark: SparkSession
    options: dict
    required_options = ["load_types", "start_date", "end_date"]

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark = spark
        self.options = options
        self.load_types = self.options.get("load_types", ["Total Actual Hourly Integrated Load"])
        self.start_date = self.options.get("start_date", "").strip()
        self.end_date = self.options.get("end_date", "").strip()
        self.user_datetime_format = "%Y-%m-%d"

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the CAISO API and parses the zip files for CSV data.

        Returns:
            Raw form of data.
        """

        logging.info(f"Getting {self.load_types} data for date {self.date}")
        start_date = datetime.strptime(self.start_date, self.user_datetime_format)
        end_date = datetime.strptime(self.end_date, self.user_datetime_format)
        end_date = end_date + timedelta(days=1)
        return self._fetch_and_parse_zip(start_date, end_date)
