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

from pyspark.sql import SparkSession
import pandas as pd
import logging
from datetime import datetime, timedelta
from . import MISODailyLoadISOSource

logging.getLogger().setLevel("INFO")


class MISOHistoricalLoadISOSource(MISODailyLoadISOSource):
    """
        The MISO Historical Load ISO Source is used to read historical load data from MISO API.

        API: <a href="https://docs.misoenergy.org/marketreports/">https://docs.misoenergy.org/marketreports/</a>

        Args:
            spark (SparkSession): Spark Session instance
            options (dict): A dictionary of ISO Source specific configurations

        Attributes:
            start_date (str): Must be in `YYYYMMDD` format.
            end_date (str): Must be in `YYYYMMDD` format.
            fill_missing (str): Set to `"true"` to fill missing Actual load with Forecast load. Default - `true`.

        """

    spark: SparkSession
    options: dict
    required_options = ["start_date", "end_date"]

    def __init__(self, spark: SparkSession, options: dict):
        super().__init__(spark, options)
        self.start_date = self.options.get("start_date", "")
        self.end_date = self.options.get("end_date", "")
        self.fill_missing = bool(self.options.get("fill_missing", "true") == "true")

    def _get_historical_data_for_date(self, date: datetime) -> pd.DataFrame:
        logging.info(f"Getting historical data for date {date}")
        df = pd.read_excel(self._fetch_from_url(f"{date.strftime(self.query_datetime_format)}_dfal_HIST.xls"),
                           skiprows=5)

        if date.month == 12 and date.day == 31:
            expected_year_rows = pd.Timestamp(date.year, 12, 31).dayofyear * 24
            received_year_rows = len(df)

            if expected_year_rows != received_year_rows:
                logging.warning(f"Didn't receive full year historical data for year {date.year}."
                                f" Expected {expected_year_rows} but Received {received_year_rows}")

        return df

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the MISO API and parses the Excel file.

        Returns:
            Raw form of data.
        """

        logging.info(f"Historical load requested from {self.start_date} to {self.end_date}")

        start_date = datetime.strptime(self.start_date, self.query_datetime_format)
        end_date = datetime.strptime(self.end_date, self.query_datetime_format) + timedelta(hours=23, minutes=59)

        dates = pd.date_range(start_date, end_date + timedelta(days=365), freq='Y', inclusive='left')
        logging.info(f"Generated date ranges are - {dates}")

        # Collect all historical data on yearly basis.
        df = pd.concat([self._get_historical_data_for_date(min(date, self.current_date))
                        for date in dates], sort=False)

        return df

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a new `date_time` column, removes null values and pivots the data.

        Args:
            df: Raw form of data received from the API.

        Returns:
            Data after basic transformations and pivoting.

        """

        df = df[df['MarketDay'] != 'MarketDay']

        # Fill missing actual values with the forecast values to avoid gaps.
        if self.fill_missing:
            df = df.fillna({'ActualLoad (MWh)': df['MTLF (MWh)']})

        df = df.rename(columns={'MarketDay': 'date',
                                'HourEnding': 'hour',
                                'ActualLoad (MWh)': 'load',
                                'LoadResource Zone': 'zone'})
        df = df.dropna()

        df['date_time'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['hour'].astype(int) - 1, 'h')

        df.drop(['hour', 'date'], axis=1, inplace=True)
        df['load'] = df['load'].astype(float)

        df = df.pivot_table(index='date_time', values='load', columns='zone').reset_index()

        df.columns = [str(x.split(' ')[0]).upper() for x in df.columns]

        return df

    def _sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter outs data outside the requested date range.

        Args:
            df: Data received after preparation.

        Returns:
            Final data after all the transformations.

        """

        start_date = datetime.strptime(self.start_date, self.query_datetime_format)
        end_date = datetime.strptime(self.end_date, self.query_datetime_format) + timedelta(hours=23, minutes=59)

        df = df[(df["DATE_TIME"] >= start_date) & (df["DATE_TIME"] <= end_date)]

        df = df.sort_values(by='DATE_TIME', ascending=True).reset_index(drop=True)

        expected_rows = ((min(end_date, self.current_date) - start_date).days + 1) * 24

        actual_rows = len(df)

        logging.info(f"Rows Expected = {expected_rows}, Rows Found = {actual_rows}")

        return df

    def _validate_options(self) -> bool:
        """
        Validates the following options:
            - `start_date` & `end_data` must be in the correct format.
            - `start_date` must be behind `end_data`.
            - `start_date` must not be in the future (UTC).

        Returns:
            True if all looks good otherwise raises Exception.

        """

        try:
            start_date = datetime.strptime(self.start_date, self.query_datetime_format)
        except ValueError:
            raise ValueError("Unable to parse Start date. Please specify in YYYYMMDD format.")

        try:
            end_date = datetime.strptime(self.end_date, self.query_datetime_format)
        except ValueError:
            raise ValueError("Unable to parse End date. Please specify in YYYYMMDD format.")

        if start_date > datetime.utcnow():
            raise ValueError("Start date can't be in future.")

        if start_date > end_date:
            raise ValueError("Start date can't be ahead of End date.")

        return True
