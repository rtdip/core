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
    The CAISO Historical Load ISO Source is used to read load data for an interval of dates
     between start_date and end_date inclusive from CAISO API.
    It supports multiple types of data. Check the `load_types` attribute.
    <br>API: <a href="http://oasis.caiso.com/oasisapi">http://oasis.caiso.com/oasisapi</a>
    <br> It creates batches of interval of 30 days and queries the CAISO API sequentially.

    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below)

    Attributes:
        load_types (list): Must be a subset of [`Demand Forecast 7-Day Ahead`, `Demand Forecast 2-Day Ahead`, `Demand Forecast Day Ahead`, `RTM 15Min Load Forecast`, `RTM 5Min Load Forecast`, `Total Actual Hourly Integrated Load`]. <br> Default Value - `[Total Actual Hourly Integrated Load]`.
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

        logging.info(f"Getting {self.load_types} data from {self.start_date} to {self.end_date}")
        start_date = datetime.strptime(self.start_date, self.user_datetime_format)
        end_date = datetime.strptime(self.end_date, self.user_datetime_format)
        end_date = end_date + timedelta(days=1)
        generated_days_ranges = []
        dates = pd.date_range(
            start_date, end_date, freq="30D", inclusive="left"
        )

        for date in dates:
            py_date = date.to_pydatetime()
            date_last = (py_date + timedelta(days=30))
            date_last = min(date_last, end_date)
            generated_days_ranges.append((py_date, date_last))

        logging.info(
            f"Generated date ranges are {generated_days_ranges}"
        )

        dfs = []
        for idx, date_range in enumerate(generated_days_ranges):
            start_date_str, end_date_str = date_range
            df = self._fetch_and_parse_zip(start_date_str, end_date_str)

            dfs.append(df)

        return pd.concat(dfs)

    def _validate_options(self) -> bool:
        try:
            datetime.strptime(self.start_date, self.user_datetime_format)
        except ValueError:
            raise ValueError(
                f"Unable to parse start_date. Please specify in {self.user_datetime_format} format."
            )

        try:
            datetime.strptime(self.end_date, self.user_datetime_format)
        except ValueError:
            raise ValueError(
                f"Unable to parse end_date. Please specify in {self.user_datetime_format} format."
            )

        return True
