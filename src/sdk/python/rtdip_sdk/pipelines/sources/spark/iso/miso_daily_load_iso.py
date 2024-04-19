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

from . import BaseISOSource
from ...._pipeline_utils.iso import MISO_SCHEMA


class MISODailyLoadISOSource(BaseISOSource):
    """
    The MISO Daily Load ISO Source is used to read daily load data from MISO API. It supports both Actual and Forecast data.

    To read more about the available reports from MISO API, download the file -
    [Market Reports](https://cdn.misoenergy.org/Market%20Reports%20Directory115139.xlsx)

    From the list of reports in the file, it pulls the report named
    `Daily Forecast and Actual Load by Local Resource Zone`.

    Actual data is available for one day minus from the given date.

    Forecast data is available for next 6 day (inclusive of given date).


    Example
    --------
    ```python
    from rtdip_sdk.pipelines.sources import MISODailyLoadISOSource
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    miso_source = MISODailyLoadISOSource(
        spark=spark,
        options={
            "load_type": "actual",
            "date": "20230520",
        }
    )

    miso_source.read_batch()
    ```

    Parameters:
        spark (SparkSession): Spark Session instance
        options (dict): A dictionary of ISO Source specific configurations (See Attributes table below)

    Attributes:
        load_type (str): Must be one of `actual` or `forecast`
        date (str): Must be in `YYYYMMDD` format.

    Please check the BaseISOSource for available methods.

    BaseISOSource:
        ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.base_iso
    """

    spark: SparkSession
    options: dict
    iso_url: str = "https://docs.misoenergy.org/marketreports/"
    query_datetime_format: str = "%Y%m%d"
    required_options = ["load_type", "date"]
    spark_schema = MISO_SCHEMA
    default_query_timezone = "US/Central"

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark = spark
        self.options = options
        self.load_type = self.options.get("load_type", "actual")
        self.date = self.options.get("date", "").strip()

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the MISO API and parses the Excel file.

        Returns:
            Raw form of data.
        """

        logging.info(f"Getting {self.load_type} data for date {self.date}")
        df = pd.read_excel(self._fetch_from_url(f"{self.date}_df_al.xls"), skiprows=4)

        return df

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Creates a new `date_time` column and removes null values.

        Args:
            df: Raw form of data received from the API.

        Returns:
            Data after basic transformations.

        """

        df.drop(
            df.index[(df["HourEnding"] == "HourEnding") | df["MISO MTLF (MWh)"].isna()],
            inplace=True,
        )
        df.rename(columns={"Market Day": "date"}, inplace=True)

        df["date_time"] = pd.to_datetime(df["date"]) + pd.to_timedelta(
            df["HourEnding"].astype(int) - 1, "h"
        )
        df.drop(["HourEnding", "date"], axis=1, inplace=True)

        data_cols = df.columns[df.columns != "date_time"]
        df[data_cols] = df[data_cols].astype(float)

        df.reset_index(inplace=True, drop=True)

        return df

    def _sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter outs Actual or Forecast data based on `load_type`.
        Args:
            df: Data received after preparation.

        Returns:
            Final data either containing Actual or Forecast values.

        """

        skip_col_suffix = ""

        if self.load_type == "actual":
            skip_col_suffix = "MTLF (MWh)"

        elif self.load_type == "forecast":
            skip_col_suffix = "ActualLoad (MWh)"

        df = df[[x for x in df.columns if not x.endswith(skip_col_suffix)]]
        df = df.dropna()
        df.columns = [str(x.split(" ")[0]).upper() for x in df.columns]

        rename_cols = {
            "LRZ1": "Lrz1",
            "LRZ2_7": "Lrz2_7",
            "LRZ3_5": "Lrz3_5",
            "LRZ4": "Lrz4",
            "LRZ6": "Lrz6",
            "LRZ8_9_10": "Lrz8_9_10",
            "MISO": "Miso",
            "DATE_TIME": "Datetime",
        }

        df = df.rename(columns=rename_cols)

        return df

    def _validate_options(self) -> bool:
        """
        Validates the following options:
            - `date` must be in the correct format.
            - `load_type` must be valid.

        Returns:
            True if all looks good otherwise raises Exception.

        """

        try:
            date = self._get_localized_datetime(self.date)
        except ValueError:
            raise ValueError("Unable to parse Date. Please specify in YYYYMMDD format.")

        if date > self.current_date:
            raise ValueError("Query date can't be in future.")

        valid_load_types = ["actual", "forecast"]

        if self.load_type not in valid_load_types:
            raise ValueError(
                f"Invalid load_type `{self.load_type}` given. Supported values are {valid_load_types}."
            )

        return True
