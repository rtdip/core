# -*- coding: utf-8 -*-
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

__author__ = ["ciaran-g", "Amber-Rigg"]

import json
import pandas as pd
import numpy as np
import os

from ecmwfapi import ECMWFService
from dotenv import load_dotenv
from joblib import Parallel, delayed

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, DoubleType, IntegerType

from .base_weather import BaseWeatherSource

load_dotenv()

class MARSDownloader:
    """
    Download nc files from ECMWF MARS server using the ECMWF python API. 
    Data is downloaded in parallel using joblib from ECMWF MARS server using the ECMWF python API.

    The following environment variables must be set:
    ECMWF_API_KEY=""
    ECMWF_API_URL="https://api.ecmwf.int/v1"
    ECMWF_API_EMAIL=""

    Attributes:
        save_path (str): Path to local directory where the nc files will be stored, in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        run_frequency (str):Frequency format of runs to download, e.g. "H"
        run_interval (str): Interval of runs, e.g. a run_frequency of "H" and run_interval of "12" will extract the data of the 00 and 12 run for each day.
    """

    def __init__(
        self,
        date_start: str,
        date_end: str,
        save_path: str,
        run_interval: str = "12",
        run_frequency: str = "H",
    ):
        self.retrieve_ran = False
        self.date_start = date_start
        self.date_end = date_end
        self.save_path = save_path
        self.format = format
        self.run_interval = run_interval
        self.run_frequency = run_frequency

        # Pandas date_list (info best retrieved per forecast day)
        self.dates = pd.date_range(
            start=date_start, end=date_end, freq=run_interval + run_frequency
        )

    def retrieve(
        self,
        mars_dict: dict,
        n_jobs=None,
        backend="loky",
        tries=5,
        cost=False,
    ):
        """Retrieve the data from the server.

        Function will use the ecmwf api to download the data from the server.
        Note that mars has a max of two active requests per user and 20 queued
        requests.
        Data is downloaded in parallel using joblib from ECMWF MARS server using the ECMWF python API.


        Args:
            mars_dict (dict): Dictionary of mars parameters.
            n_jobs (int, optional): Download in parallel? by default None, i.e. no parallelization
            backend (str, optional) : Specify the parallelization backend implementation in joblib, by default "loky"
            tries (int, optional): Number of tries for each request if it fails, by default 5
            cost (bool, optional):  Pass a cost request to mars to estimate the size and efficiency of your request,
                but not actually download the data. Can be useful for defining requests,
                by default False.

        Returns:
            self: reference to self
        """
        chk = ["date", "target", "time", "format", "output"]
        for i in chk:
            if i in mars_dict.keys():
                raise ValueError(f"don't include {i} in the mars_dict")

        parallel = Parallel(n_jobs=n_jobs, backend=backend)

        def _retrieve_datetime(i, j, cost=cost):
            i_dict = {"date": i, "time": j}

            if cost:
                filename = f"{i}_{j}.txt"
            else:
                filename = f"{i}_{j}.nc"
                i_dict["format"] = "netcdf"

            target = os.path.join(self.save_path, filename)
            msg = f"retrieving mars data --- {filename}"

            req_dict = {**i_dict, **mars_dict}
            for k, v in req_dict.items():
                if isinstance(v, (list, tuple)):
                    req_dict[k] = "/".join([str(x) for x in v])

            req_dict = ["{}={}".format(k, v) for k, v in req_dict.items()]
            if cost:
                req_dict = "list,output=cost,{}".format(",".join(req_dict))
            else:
                req_dict = "retrieve,{}".format(",".join(req_dict))

            for j in range(tries):
                try:
                    print(msg)
                    server = ECMWFService("mars")
                    server.execute(req_dict, target)
                    return 1
                except:
                    if j < tries - 1:
                        continue
                    else:
                        return 0

        self.success = parallel(
            delayed(_retrieve_datetime)(str(k.date()), f"{k.hour:02}")
            for k in self.dates
        )
        self.retrieve_ran = True

        return self

    def info(self) -> pd.Series:
        """
        Return info on each ECMWF request.

        Returns:
            pd.Series: Successful request for each run == 1.
        """
        if not self.retrieve_ran:
            raise ValueError(
                "Before using self.info(), prepare the request using "
                + "self.retrieve()"
            )
        y = pd.Series(self.success, index=self.dates, name="success", dtype=bool)
        return y
    

class WeatherForecastECMWFV1Source(BaseWeatherSource):
    """
    The Weather Forecast API V1 Source class to doownload nc files from ECMWF MARS server using the ECMWF python API. 
    
    The following environment variables must be set:
    ECMWF_API_KEY=""
    ECMWF_API_URL="https://api.ecmwf.int/v1"
    ECMWF_API_EMAIL=""
    Args:
        spark (SparkSession): Spark Session instance

    Attributes:
        save_path (str): Path to local directory where the nc files will be stored, in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format    date_end:str, 
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        ecmwf_class (str): ecmwf classification of data
        stream (str): Operational model stream
        expver (str): Version of data
        leveltype (str): Surface level forecasts
        ec_vars (list): Variables of forecast measurements.
        forecast_area (list): N/W/S/E coordinates of the forecast area
    """

    spark: SparkSession
    options: dict
    weather_url: str = "https://api.ecmwf.int/v1"

    def __init__(
            self, 
            spark: SparkSession, 
            sc, 
            save_path: str,
            date_start:str, 
            date_end:str, 
            ecmwf_class: str, 
            stream: str, 
            expver: str, 
            leveltype: str, 
            ec_vars: list, 
            forecast_area: list) -> None:
        
        self.spark = spark
        self.dbutils = dbutils 
        self.sc = sc

        self.save_path = save_path
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
            save_path=self.save_path,
            run_interval="12",
            run_frequency="H"
            )
        
        ec_conn.retrieve(
            mars_dict= self._get_api_params(_get_lead_times()),
            tries=5,
            n_jobs=-1, # maximum of 20 queued requests per user (only two allowed active)
            )

        return 
    
    
