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
import time
from datetime import datetime, timedelta
from io import BytesIO

import pandas as pd
from pyspark.sql import SparkSession

from . import PJMDailyLoadISOSource


class PJMHistoricalLoadISOSource(PJMDailyLoadISOSource):
    """
    The PJM Historical Load ISO Source is used to read historical load data from PJM API.

    To read more about the reports, visit the following URLs -
    <br>
    Actual doc:    [ops_sum_prev_period](https://dataminer2.pjm.com/feed/ops_sum_prev_period/definition)
    <br>
    Forecast doc:  [load_frcstd_7_day](https://dataminer2.pjm.com/feed/load_frcstd_7_day/definition)

    Historical is the same PJM endpoint as Actual, but is called repeatedly within a range established by the
    start_date & end_date attributes

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.sources import PJMHistoricalLoadISOSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    pjm_source = PJMHistoricalLoadISOSource(
        spark=spark,
        options={
            "api_key": "{api_key}",
            "start_date": "20230510",
            "end_date": "20230520",
        }
    )

    pjm_source.read_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below)

    Attributes:
        api_key (str): Must be a valid key from PJM, see PJM documentation
        start_date (str): Must be in `YYYY-MM-DD` format.
        end_date (str): Must be in `YYYY-MM-DD` format.

        query_batch_days (int): (optional) Number of days must be < 160 as per PJM & is defaulted to `120`
        sleep_duration (int): (optional) Number of seconds to sleep between request, defaulted to `5` seconds, used to manage requests to PJM endpoint
        request_count (int): (optional) Number of requests made to PJM endpoint before sleep_duration, currently defaulted to `1`

    Please check the BaseISOSource for available methods.

    BaseISOSource:
        ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.base_iso"""

    spark: SparkSession
    options: dict
    required_options = ["api_key", "start_date", "end_date"]

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark: SparkSession = spark
        self.options: dict = options
        self.api_key: str = self.options.get("api_key", "").strip()
        self.start_date: str = self.options.get("start_date", "")
        self.end_date: str = self.options.get("end_date", "")
        self.query_batch_days: int = self.options.get("query_batch_days", 120)
        self.sleep_duration: int = self.options.get("sleep_duration", 5)
        self.request_count: int = self.options.get("request_count", 1)
        self.load_type: str = "actual"
        self.user_datetime_format = "%Y-%m-%d"

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the PJM API and parses the return including date ranges.

        Returns:
            Raw form of data.
        """

        logging.info(
            f"Historical load requested from {self.start_date} to {self.end_date}"
        )
        start_date = datetime.strptime(self.start_date, self.user_datetime_format)
        end_date = datetime.strptime(self.end_date, self.user_datetime_format).replace(
            hour=23
        )

        days_diff = (end_date - start_date).days
        logging.info(f"Expected hours for a single zone = {(days_diff + 1) * 24}")
        generated_days_ranges = []
        dates = pd.date_range(
            start_date, end_date, freq=pd.DateOffset(days=self.query_batch_days)
        )

        for date in dates:
            py_date = date.to_pydatetime()
            date_last = (py_date + timedelta(days=self.query_batch_days - 1)).replace(
                hour=23
            )
            date_last = min(date_last, end_date)
            generated_days_ranges.append((py_date, date_last))

        logging.info(
            f"Generated date ranges for batch days {self.query_batch_days} are {generated_days_ranges}"
        )

        # Collect all historical data on yearly basis.
        dfs = []
        for idx, date_range in enumerate(generated_days_ranges):
            start_date_str = date_range[0].strftime(self.query_datetime_format)
            end_date_str = date_range[1].strftime(self.query_datetime_format)

            df = pd.read_csv(
                BytesIO(self._fetch_from_url("", start_date_str, end_date_str))
            )
            dfs.append(df)

            if idx > 0 and idx % self.request_count == 0:
                logging.info(f"Going to sleep for {self.sleep_duration} seconds")
                time.sleep(self.sleep_duration)

        df = pd.concat(dfs, sort=False)
        df = df.reset_index(drop=True)
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

        if start_date > datetime.utcnow() - timedelta(days=1):
            raise ValueError("Start date can't be in future.")

        if start_date > end_date:
            raise ValueError("Start date can't be ahead of End date.")

        if end_date > datetime.utcnow() - timedelta(days=1):
            raise ValueError("End date can't be in future.")

        if self.sleep_duration < 0:
            raise ValueError("Sleep duration can't be negative.")

        if self.request_count < 0:
            raise ValueError("Request count can't be negative.")

        if self.query_batch_days < 0:
            raise ValueError("Query batch days count can't be negative.")

        return True
