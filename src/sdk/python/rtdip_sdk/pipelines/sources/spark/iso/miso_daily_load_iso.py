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
import pandas as pd
from pyspark.sql import SparkSession
from datetime import datetime

from ...._pipeline_utils.iso import MISO_SCHEMA
from . import BaseISOSource


class MISODailyLoadISOSource(BaseISOSource):

    spark: SparkSession
    options: dict
    iso_url: str = "https://docs.misoenergy.org/marketreports/"
    query_datetime_format: str = "%Y%m%d"
    required_options: list[str] = ["load_type", "date"]
    spark_schema = MISO_SCHEMA

    def __init__(self, spark: SparkSession, options: dict) -> None:

        super().__init__(spark, options)
        self.spark = spark
        self.options = options
        self.load_type = self.options.get("load_type", "actual")
        self.date_str = self.options.get("date", "").strip()

    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:

        df.drop(df.index[(df['HourEnding'] == 'HourEnding') | df['MISO MTLF (MWh)'].isna()], inplace=True)
        df.rename(columns={'Market Day': 'date'}, inplace=True)

        df['date_time'] = pd.to_datetime(df['date']) + pd.to_timedelta(df['HourEnding'].astype(int) - 1, 'h')
        df.drop(['HourEnding', 'date'], axis=1, inplace=True)

        data_cols = df.columns[df.columns != 'date_time']
        df[data_cols] = df[data_cols].astype(float)

        df.reset_index(inplace=True, drop=True)

        return df

    def sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:

        skip_col_suffix = ""

        if self.load_type == "actual":
            skip_col_suffix = 'MTLF (MWh)'

        elif self.load_type == "forecast":
            skip_col_suffix = 'ActualLoad (MWh)'

        df = df[[x for x in df.columns if not x.endswith(skip_col_suffix)]]
        df = df.dropna()
        df.columns = [str(x.split(' ')[0]).upper() for x in df.columns]

        return df

    def pull_data(self) -> pd.DataFrame:

        logging.info(f"Getting {self.load_type} data for date {self.date_str}")
        df = pd.read_excel(self.fetch_from_url(f"{self.date_str}_df_al.xls"), skiprows=4)

        return df

    def validate_options(self) -> bool:

        try:
            datetime.strptime(self.date_str, self.query_datetime_format)
        except ValueError:
            raise ValueError(f"Unable to parse Date. Please specify in YYYYMMDD format.")

        valid_load_types = ["actual", "forecast"]

        if self.load_type not in valid_load_types:
            raise ValueError(f"Invalid load_type `{self.load_type}` given. Supported values are {valid_load_types}.")

        return True
