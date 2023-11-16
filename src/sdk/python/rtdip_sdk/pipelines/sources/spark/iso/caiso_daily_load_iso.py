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
from _datetime import datetime, timedelta
import logging
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
from pyspark.sql import SparkSession

from ...._pipeline_utils.iso import CAISO_SCHEMA
from . import BaseISOSource


class CAISODailyLoadISOSource(BaseISOSource):
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
        date (str): Must be in `YYYY-MM-DD` format.

    Please check the BaseISOSource for available methods.

    BaseISOSource:
        ::: src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso.base_iso
    """

    spark: SparkSession
    options: dict
    iso_url: str = "http://oasis.caiso.com/oasisapi/SingleZip"
    query_datetime_format: str = "%Y%m%dT00:00-0000"
    required_options = ["load_types", "date"]
    spark_schema = CAISO_SCHEMA
    default_query_timezone = "UTC"

    def __init__(self, spark: SparkSession, options: dict) -> None:
        super().__init__(spark, options)
        self.spark = spark
        self.options = options
        self.load_types = self.options.get("load_types", ["Total Actual Hourly Integrated Load"])
        self.date = self.options.get("date", "").strip()
        self.user_datetime_format = "%Y-%m-%d"

    def _pull_data(self) -> pd.DataFrame:
        """
        Pulls data from the CAISO API and parses the zip files for CSV data.

        Returns:
            Raw form of data.
        """

        logging.info(f"Getting {self.load_types} data for date {self.date}")
        start_date = datetime.strptime(self.date, self.user_datetime_format)
        end_date = start_date + timedelta(days=1)
        return self._fetch_and_parse_zip(start_date, end_date)

    def _fetch_and_parse_zip(self, start_date: datetime, end_date: datetime) -> pd.DataFrame:
        suffix = (
            f"?resultformat=6&"
            f"queryname=SLD_FCST&"
            "version=1&"
            f"startdatetime={start_date.strftime(self.query_datetime_format)}&"
            f"enddatetime={end_date.strftime(self.query_datetime_format)}"
        )

        content = self._fetch_from_url(suffix)
        if not content:
            raise Exception(f"Empty Response was returned")
        logging.info(f"Unzipping the file")

        zf = ZipFile(BytesIO(content))

        csvs = list(filter(lambda name: ".csv" in name, zf.namelist()))
        if len(csvs) == 0:
            raise Exception("No data was found in the specified interval")

        df = pd.read_csv(zf.open(csvs[0]))
        return df

    def _prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:

        date_cols = ['INTERVALSTARTTIME_GMT', 'INTERVALENDTIME_GMT']
        for date_col in date_cols:
            df[date_col] = df[date_col].apply(lambda data: datetime.strptime(str(data)[:19], "%Y-%m-%dT%H:%M:%S"))

        df = df.rename(
            columns={
                "INTERVALSTARTTIME_GMT": "StartTime",
                "INTERVALENDTIME_GMT": "EndTime",
                "LOAD_TYPE": "LoadType",
                "OPR_DT": "OprDt",
                "OPR_HR": "OprHr",
                "OPR_INTERVAL": "OprInterval",
                "MARKET_RUN_ID": "MarketRunId",
                "TAC_AREA_NAME": "TacAreaName",
                "LABEL": "Label",
                "XML_DATA_ITEM": "XmlDataItem",
                "POS": "Pos",
                "MW": "Load",
                "EXECUTION_TYPE": "ExecutionType",
                "GROUP": "Group",
            }
        )

        return df

    def _sanitize_data(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[df['Label'].isin(self.load_types)]
        return df
