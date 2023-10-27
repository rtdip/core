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


import pandas as pd
import numpy as np

from ...interfaces import SourceInterface
from ...._pipeline_utils.models import Libraries, SystemType
from .base_mars import SparkECMWFBaseMarsSource

from pyspark.sql import SparkSession


class SparkECMWFWeatherForecastSource(SourceInterface):
    """
    The Weather Forecast API V1 Source class to doownload nc files from ECMWF MARS server using the ECMWF python API.

    Parameters:
        spark (SparkSession): Spark Session instance
        save_path (str): Path to local directory where the nc files will be stored, in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format    date_end:str,
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        ecmwf_class (str): ecmwf classification of data
        stream (str): Operational model stream
        expver (str): Version of data
        leveltype (str): Surface level forecasts
        ec_vars (list): Variables of forecast measurements.
        forecast_area (list): N/W/S/E coordinates of the forecast area
        ecmwf_api_key (str): API key for ECMWF API
        ecmwf_api_email (str): Email for ECMWF API
    """

    spark: SparkSession

    def __init__(
        self,
        spark: SparkSession,
        save_path: str,
        date_start: str,
        date_end: str,
        ecmwf_class: str,
        stream: str,
        expver: str,
        leveltype: str,
        ec_vars: list,
        forecast_area: list,
        ecmwf_api_key: str,
        ecmwf_api_email: str,
    ) -> None:
        self.spark = spark
        self.save_path = save_path
        self.date_start = date_start
        self.date_end = date_end
        self.ecmwf_class = ecmwf_class
        self.stream = stream  # operational model
        self.expver = expver  # experiment version of data
        self.leveltype = leveltype  # surface level forecasts
        self.ec_vars = ec_vars  # variables
        self.forecast_area = forecast_area  # N/W/S/E
        self.ecmwf_api_key = ecmwf_api_key
        self.ecmwf_api_email = ecmwf_api_email

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_read_validation(self):
        return True

    def post_read_validation(self):
        return True

    def read_stream(self):
        return True

    @classmethod
    def _get_lead_time(cls):
        """
        Lead time for the forecast data.
        90 hours - 1 Hour Interval
        90-146 - 3 Hour interval
        146 -246 - 6 Hour interval

        Returns:
            lead_times: Lead times in an array format.
        """
        lead_times = [*range(91), *range(93, 146, 3), *range(150, 246, 6)]
        np.array(lead_times)

        return lead_times

    def _get_api_params(self, lead_times):
        """
        API parameters for the forecast data.

        Returns:
            params (dict): API parameters for the forecast data.
        """

        params = {
            "class": self.ecmwf_class,  # ecmwf classification of data
            "stream": self.stream,  # operational model
            "expver": self.expver,  # experiment version of data
            "levtype": self.leveltype,  # surface level forecasts
            "type": "fc",  # forecasts
            "param": self.ec_vars,  # variables
            "step": lead_times,  # which lead times to download
            "area": self.forecast_area,  # N/W/S/E
            "grid": [0.1, 0.1],  # grid res of output
        }

        return params

    def read_batch(self):
        """
        Pulls data from the Weather API and returns as .nc files.

        """
        lead_times = self._get_lead_time()
        para = self._get_api_params(lead_times=lead_times)

        ec_conn = SparkECMWFBaseMarsSource(
            date_start=self.date_start,
            date_end=self.date_end,
            save_path=self.save_path,
            run_interval="12",
            run_frequency="H",
            ecmwf_api_key=self.ecmwf_api_key,
            ecmwf_api_email=self.ecmwf_api_email,
            ecmwf_api_url="https://api.ecmwf.int/v1",
        )

        ec_conn.retrieve(
            mars_dict=para,
            tries=5,
            n_jobs=-1,  # maximum of 20 queued requests per user (only two allowed active)
        )
