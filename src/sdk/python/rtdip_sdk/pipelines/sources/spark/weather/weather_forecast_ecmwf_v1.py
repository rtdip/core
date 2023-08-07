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

import json
import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, IntegerType
import numpy as np

from .base_weather import BaseWeatherSource
from .ECMWF.MARS_api_download import MARSDownloader


class WeatherForecastECMWFV1Source(BaseWeatherSource):
    """
    The Weather Forecast API V1 Source class.

    URL: <a /a>

    Args:
        spark (SparkSession): Spark Session instance

    Attributes:


    """

    spark: SparkSession
    options: dict
    weather_url: str = "https://api.ecmwf.int/v1"

    def __init__(self, spark: SparkSession, sc, date_start:str, date_end:str, ecmwf_class: str, stream: str, expver: str, leveltype: str, ec_vars: list, forecast_area: list) -> None:
        
        self.spark = spark
        self.dbutils = dbutils 
        self.sc = sc

        self.date_start = date_start
        self.date_end = date_end
        self.ecmwf_class = ecmwf_class
        self.stream = stream, # operational model
        self.expver = expver # experiment version of data
        self.leveltype = leveltype, # surface level forecasts
        self.ec_vars = ec_vars, # variables
        self.forecast_area =  forecast_area, # N/W/S/E

        self.set_spark_conf()
    
    def _get_lead_time(self):
        """
        Lead time for the forecast data.
        90 hours - 1 Hour Interval
        90-146 - 3 Hour interval
        146 -246 - 6 Hour interval

        Returns:
            lead_times in array format.
        """
        lead_times = [*range(91), *range(93, 146, 3), *range(150, 246, 6)]
        np.array(lead_times)

        return lead_times
    
    def _get_api_params(self, lead_times):
        params = {
        "class": self.ecmwf_class, # ecmwf classification of data
        "stream": self.stream, # operational model
        "expver": self.expver, # experiment version of data
        "levtype": self.leveltype, # surface level forecasts
        "type": "fc", # forecasts
        "param": self.ec_vars, # variables
        "step": lead_times, # which lead times?
        "area": self.forecast_area, # N/W/S/E
        "grid": [0.1, 0.1], # grid res of output
        }

        return params
    

    def _pull_data(self):
        """
        Pulls data from the Weather API and returns as .nc files.

        Returns:
            Raw form of data.
        """

        ec_conn = MARSDownloader(
            date_start=self.date_start,
            date_end= self.date_end,
            save_path="../data/ecmwf/oper/fc/sfc/europe/",
            run_interval="12",
            run_frequency="H"
            )
        
        ec_conn.retrieve(
            mars_dict= self._get_api_params(self.ec_vars, self.lead_times),
            tries=5,
            n_jobs=-1, # maximum of 20 queued requests per user (only two allowed active)
            )

        return 
