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
    spark: SparkSession
    options: dict
    requires_options = ["start_date", "end_date"]

    def __init__(self, spark: SparkSession, options: dict):
        super().__init__(spark, options)
        self.start_date = self.options.get("start_date", "")
        self.end_date = self.options.get("end_date", "")
        self.fill_missing_actual = bool(self.options.get("fill_missing_actual", "true") == "true")

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:

        df = df[df['MarketDay'] != 'MarketDay']

        # Fill missing actual values with the forecast values to avoid gaps.
        if self.fill_missing_actual:
            df['ActualLoad (MWh)'] = df['ActualLoad (MWh)'].fillna(df['MTLF (MWh)'])

        df.rename(columns={'MarketDay': 'date', 'HourEnding': 'hour', 'ActualLoad (MWh)': 'load', 'LoadResource Zone': 'zone'},
                  inplace=True)
        df = df.dropna()

        df['date_time'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['hour'].astype(int) - 1, 'h')

        df.drop(['hour', 'date'], axis=1, inplace=True)
        df['load'] = df['load'].astype(float)

        df = df.pivot_table(index='date_time', values='load', columns='zone').reset_index()

        df.columns = [str(x.split(' ')[0]).upper() for x in df.columns]
        return df

    def get_historical_data_for_date(self, date: datetime) -> pd.DataFrame:
        logging.info(f"Getting historical data for date {date}")
        df = pd.read_excel(self.fetch_from_url(f"{date.strftime(self.query_datetime_format)}_dfal_HIST.xls"),
                           skiprows=5)

        if date.month == 12 and date.day == 31:

            expected_year_rows = pd.Timestamp(date.year, 12, 31).dayofyear * 24
            received_year_rows = len(df)

            if expected_year_rows != received_year_rows:
                logging.warning(f"Didn't receive full year historical data for year {date.year}."
                                f" Expected {expected_year_rows} but Received {received_year_rows}")

        return df

    def pull_data(self) -> pd.DataFrame:

        logging.info(f"Historical load requested from {self.start_date} to {self.end_date}")

        start_date = datetime.strptime(self.start_date, self.query_datetime_format)
        end_date = datetime.strptime(self.end_date, self.query_datetime_format) + timedelta(hours=23, minutes=59)

        dates = pd.date_range(start_date, end_date + timedelta(days=365), freq='Y', inclusive='left')

        if len(dates) == 0:
            raise Exception(f"No dates are generated, please check start and end date.")

        logging.info(f"Generated date ranges are - {dates}")

        # Collect all historical data on yearly basis.
        df = pd.concat([self.get_historical_data_for_date(min(date, self.current_date))
                        for date in dates], sort=False)

        return df

    def sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:

        start_date = datetime.strptime(self.start_date, self.query_datetime_format)
        end_date = datetime.strptime(self.end_date, self.query_datetime_format)

        # Filter out data outside of the expected range.
        df = df[(df["DATE_TIME"] >= start_date) & (df["DATE_TIME"] <= end_date)]

        df = df.sort_values(by='DATE_TIME', ascending=True).reset_index(drop=True)

        expected_rows = ((min(end_date, self.current_date) - start_date).days + 1) * 24

        actual_rows = len(df)

        logging.info(f"Rows Expected = {expected_rows}, Rows Found = {actual_rows}")


        return df

    def validate_options(self) -> bool:

        try:
            start_date = datetime.strptime(self.start_date, self.query_datetime_format)
        except ValueError:
            logging.error(f"Unable to parse Start date. Please specify in YYYYMMDD format.")
            return False

        try:
            end_date = datetime.strptime(self.end_date, self.query_datetime_format)
        except ValueError:
            logging.error(f"Unable to parse End date. Please specify in YYYYMMDD format.")
            return False

        if start_date > datetime.utcnow():
            logging.error(f"Start date can't be in future.")
            return False

        if start_date > end_date:
            logging.error(f"Invalid options provide. Start date can't be ahead of End date.")
            return False

        return True
