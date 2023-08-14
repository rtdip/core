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

import os
import pandas as pd
import numpy as np
import xarray as xr

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, explode, to_timestamp, when, lit, coalesce
from pyspark.sql.types import ArrayType, StringType

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.weather_ecmwf import RTDIP_STRING_WEATHER_DATA_MODEL, RTDIP_FLOAT_WEATHER_DATA_MODEL 


class ExtractBase:
    """
    Base class for extracting forecast data downloaded in .nc format from ECMWF.

    Attributes:
        load_path (str): Path to local directory where the nc files will be stored, in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        run_frequency (str):Frequency format of runs to download, e.g. "H"
        run_interval (str): Interval of runs, e.g. a run_frequency of "H" and run_interval of "12" will extract the data of the 00 and 12 run for each day.
        lat (xr.DataArray) : Latitude values to extract from nc files
        lon (xr.DataArray) : Longitude values to extract from nc files
        utc (bool = True): Whether to convert the time to UTC or not
    """

    def __init__(
        self,
        load_path: str,
        date_start: str,
        date_end: str,
        run_interval: str,
        run_frequency: str,
        lat: xr.DataArray,
        lon: xr.DataArray,
        utc: bool = True,
    ):
        self.load_path = load_path
        self.lat = lat
        self.lon = lon
        self.date_start = date_start
        self.date_end = date_end
        self.run_frequency = run_frequency
        self.run_interval = run_interval
        self.utc = utc
        self.dates = pd.date_range(
            start=self.date_start,
            end=self.date_end,
            freq=self.run_interval + self.run_frequency,
        )
    
    def convert_ws_tag_names(TagList: list):
        """
        Converts the tag names of wind speed from the format used in the nc files to the format used in the weather data model.

        Attributes:
            Tag List (list): List of variable names of raw tags to be extracted from the nc files

        Returns:
            new_tags(list): List of variable names of raw tags to be extracted from the nc files, converted to the format used in the weather data model.
        """
        convert_dict = {
        "10u": "u10",
        "100u": "u100",
        "200u": "u200",
        "10v": "v10",
        "100v": "v100",
        "200v": "v200",
        }
        new_tags = [convert_dict[i] if i in convert_dict.keys() else i for i in TagList]
        return new_tags

    def raw(self, variables: list, method: str = "nearest") -> pd.DataFrame:
        """Extract raw data from stored nc filed downloaded via ECMWF MARS.

        Attributes:
            variables (list): List of variable names of raw tags to be extracted from the nc files
            method (str, optional): The method used to match latitude/longitude in xarray using .sel(), by default "nearest"

        Returns:
            df (pd.DataFrame): Raw data extracted with lat, lon, run_time, target_time as a pd.multiindex and variables as columns.
        """
        df = []
        # e.g. 10u variable is saved as u10 in the file...
        vars_processed = convert_ws_tag_names(variables)

        for i in self.dates:
            filename = f"{str(i.date())}_{i.hour:02}.nc"
            fullpath = os.path.join(self.load_path, filename)
            ds = xr.open_dataset(fullpath)
            tmp = (
                ds[vars_processed]
                .sel(latitude=self.lat, longitude=self.lon, method=method)
                .to_dataframe()
            )
            tmp["run_time"] = i
            df.append(tmp)
            ds.close()

        df = pd.concat(df, axis=0)

        df = df.rename_axis(
            index={
                "time": "target_time",
                "latitude": "lat",
                "longitude": "lon",
            }
        )

        df = df.reset_index(["lat", "lon"])
        df[["lat", "lon"]] = df[["lat", "lon"]].apply(
            lambda x: np.round(x.astype(float), 5)
        )
        df = df.reset_index().set_index(["lat", "lon", "run_time", "target_time"])

        if self.utc:
            df = df.tz_localize("UTC", level=-1)
            df = df.tz_localize("UTC", level=-2)

        df = df[~(df.index.duplicated(keep="first"))]
        df = df.sort_index(axis=0)
        df = df.sort_index(axis=1)

        return df


class ExtractPoint(ExtractBase):
    """Extract a single point from a local .nc file downloaded from ecmwf via mars

    Attributes:
        lat (float): Latitude of point to extract
        lon (float): Longitude of point to extract
        load_path (str): Path to local directory with nc files downloaded in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        run_frequency (str): Frequency format of runs to download, e.g. "H"
        run_interval (str): Interval of runs, e.g. a run_frequency of "H" and run_interval of "12" will extract the data of the 00 and 12 run for each day.
        utc (bool, optional): Add utc to the datetime indexes? Defaults to True.
    """

    def __init__(
        self,
        lat: float,
        lon: float,
        load_path: str,
        date_start: str,
        date_end: str,
        run_interval: str,
        run_frequency: str,
        utc: bool = True,
    ):
        lat_xr = xr.DataArray([lat], dims=["latitude"])
        lon_xr = xr.DataArray([lon], dims=["longitude"])

        self.lat = lat_xr
        self.lon = lon_xr
        self.load_path = load_path
        self.date_start = date_start
        self.date_end = date_end
        self.run_frequency = run_frequency
        self.run_interval = run_interval
        self.utc = utc

        super(ExtractPoint, self).__init__(
            lat=lat_xr,
            lon=lon_xr,
            load_path=load_path,
            date_start=date_start,
            date_end=date_end,
            run_interval=run_interval,
            run_frequency=run_frequency,
            utc=utc,
        )


class ExtractGrid(ExtractBase):
    """Extract a grid from a local .nc file downloaded from ecmwf via mars

    Attributes:
        lat_min (float): Minimum latitude of grid to extract
        lat_max (float): Maximum latitude of grid to extract
        lon_min (float): Minimum longitude of grid to extract
        lon_max (float): Maximum longitude of grid to extract
        grid_step (float): The grid length to use to define the grid, e.g. 0.1.
        load_path (str): Path to local directory with nc files downloaded in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        run_frequency (str): Frequency format of runs to download, e.g. "H"
        run_interval (str): Interval of runs, e.g. a run_frequency of "H" and run_interval of "12" will extract the data of the 00 and 12 run for each day.
        utc (bool, optional): Add utc to the datetime indexes? Defaults to True.

    """

    def __init__(
        self,
        lat_min: float,
        lat_max: float,
        lon_min: float,
        lon_max: float,
        grid_step: float,
        load_path: str,
        date_start: str,
        date_end: str,
        run_interval: str,
        run_frequency: str,
        utc: bool = True,
    ):
        # hmm careful with floating points, this seems to work ok...
        lat_xr = xr.DataArray(
            np.linspace(
                lat_min, lat_max, int(np.round((lat_max - lat_min) / grid_step)) + 1
            ),
            dims=["latitude"],
        )
        lon_xr = xr.DataArray(
            np.linspace(
                lon_min, lon_max, int(np.round((lon_max - lon_min) / grid_step)) + 1
            ),
            dims=["longitude"],
        )

        self.load_path = load_path
        self.lat_min = lat_min
        self.lat_max = lat_max
        self.lon_min = lon_min
        self.lon_max = lon_max
        self.grid_step = grid_step
        self.lat = lat_xr
        self.lon = lon_xr
        self.date_start = date_start
        self.date_end = date_end
        self.run_frequency = run_frequency
        self.run_interval = run_interval
        self.utc = utc

        super(ExtractGrid, self).__init__(
            lat=lat_xr,
            lon=lon_xr,
            load_path=load_path,
            date_start=date_start,
            date_end=date_end,
            run_interval=run_interval,
            run_frequency=run_frequency,
            utc=utc,
        )

    def aggregate(
        self,
        fun_dict: dict,
        **kwargs,
    ) -> pd.DataFrame:
        """Aggregate multiple grid points to a single point using functions.

        Attributes:
        fun_dict : dict
            Dictionary containing keys of the column names and values are a list of
            the names of or functions to apply. See pd.DataFrame.aggregate()
        `**kwargs` : Keyword arguments to pass on to the .process() and .raw()
            functions, e.g. variables, decumulate_cols, etc.

        Returns:
        df (pd.DataFrame): Processed and aggregated data extracted with lat, lon, run_time, target_time
            as a pd.multiindex and variables as columns.
        """
        df = self.process(**kwargs)

        df_ll = (
            df.reset_index(["lat", "lon"])[["lat", "lon"]]
            .groupby(level=["run_time", "target_time"])
            .mean()
        )

        df = df.groupby(level=["run_time", "target_time"], as_index=True).aggregate(
            fun_dict
        )
        df.columns = ["_grid_".join(i) for i in df.columns]

        df = pd.concat([df, df_ll], axis=1)
        df = df.reset_index().set_index(["lat", "lon", "run_time", "target_time"])
        df = df.sort_index(axis=0)
        df = df.sort_index(axis=1)

        return df


class WeatherForecastECMWFV1SourceTransformer(TransformerInterface):
    '''
    Extract a single point from a local .nc file downloaded from ecmwf via mars or a grid from a local .nc file downloaded from ecmwf via mars and transform to the RTDIP weather data model.
    
    Atributes:
        load_path (str): Path to local directory with nc files downloaded in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        lat (float): Latitude of point to extract
        lon (float): Longitude of point to extract
        lat_min (float): Minimum latitude of grid to extract requried for grid extraction
        lat_max (float): Maximum latitude of grid to extract requried for grid extraction
        lon_min (float): Minimum longitude of grid to extract requried for grid extraction
        lon_max (float): Maximum longitude of grid to extract requried for grid extraction
        variable_list: list, List of forecast MARS variables to extract
        TAG_PREFIX: str, Tag prefix for the weather data model TagName
        Source: str, Source of Forecast ie ECMWF

   '''

    def __init__(
        self,
        load_path: str,
        date_start: str,
        date_end: str,
        lat: float,
        lon: float,
        lat_min: float,
        lon_min: float,
        lat_max: float,
        lon_max: float,
        variable_list: list, 
        TAG_PREFIX: str,
        Source: str, 
    ):
        self.load_path = load_path
        self.date_start = date_start
        self.date_end = date_end
        self.lat = lat
        self.lon = lon
        self.lat_min = lat_min
        self.lon_min = lon_min
        self.lat_max = lat_max
        self.lon_max = lon_max
        self.variable_list = variable_list
        self.TAG_PREFIX = TAG_PREFIX
        self.Source = Source

    def nc_transform_point(self):

        '''
        Extract a single point from a local .nc file downloaded from ecmwf via mars and transform to the RTDIP weather data model.
            
        Returns:
            df_final (pd.DataFrame): Processed raw data extracted with Tagname, Latitude, Longitude, EnqueuedTime, EventTime, EventDate, Value, Source, Status and Latest.
        '''

        data_conn = ExtractPoint(
        lat=self.lat,
        lon=self.lon,
        load_path = self.load_path,
        date_start = self.date_start,
        date_end=self.date_end,
        run_interval="12",
        run_frequency="H",
        utc=True,
    )

        df = data_conn.raw(
        variables=self.variable_list,
        method="nearest"
        )

        vars_processed = ExtractBase.convert_ws_tag_names(self.variable_list)

        df_new = df.reset_index()

        df_new = df_new.rename(columns={"lat": "Latitude", "lon": "Longitude", "run_time": "EnqueuedTime","target_time": "EventTime"})

        df_new = (df_new.set_index(['Latitude','Longitude','EnqueuedTime','EventTime'])[vars_processed]
            .rename_axis('Measure', axis=1)
            .stack()
            .reset_index(name='Value'))
        
        df_new['Source'] = self.Source
        df_new['Status'] = "Good"
        df_new['Latest'] = True
        
        df_new['EventDate'] = pd.to_datetime(df_new["EventTime"]).dt.date
        df_new['TagName'] = self.TAG_PREFIX + df_new["Latitude"].astype(str) + "_" + df_new["Longitude"].astype(str) + "_" + df_new["Source"] + "_" + df_new["Measure"]
        df_final = df_new.drop('Measure', axis=1)
        
        return df_final
    

    def nc_transform_grid(self):

        '''
        Extract a grid from a local .nc file downloaded from ecmwf via mars and transform to the RTDIP weather data model.
            
        Returns:
            df_final (pd.DataFrame): Processed raw data extracted with Tagname, Latitude, Longitude, EnqueuedTime, EventTime, EventDate, Value, Source, Status and Latest.
        '''

        data_conn = ExtractGrid(
        lat_max=self.lat_max,
        lat_min=self.lat_min,
        lon_max=self.lon_max,
        lon_min=self.lon_min,
        grid_step=0.1,
        load_path=self.load_path,
        date_start=self.date_start,
        date_end=self.date_end,
        run_interval="12",
        run_frequency="H",
        utc=True
    )

        df = data_conn.raw(
        variables=self.variable_list,
        method="nearest"
        )

        vars_processed = ExtractBase.convert_ws_tag_names(self.variable_list)

        df_new = df.reset_index()

        df_new = df_new.rename(columns={"lat": "Latitude", "lon": "Longitude", "run_time": "EnqueuedTime","target_time": "EventTime"})

        df_new = (df_new.set_index(['Latitude','Longitude','EnqueuedTime','EventTime'])[vars_processed]
            .rename_axis('Measure', axis=1)
            .stack()
            .reset_index(name='Value'))
        
        df_new['Source'] = self.Source
        df_new['Status'] = "Good"
        df_new['Latest'] = True

        df_new['EventDate'] = pd.to_datetime(df_new["EventTime"]).dt.date
        df_new['TagName'] = self.TAG_PREFIX + df_new["Latitude"].astype(str) + "_" + df_new["Longitude"].astype(str) + "_" + df_new["Source"] + "_" + df_new["Measure"]
        df_final = df_new.drop('Measure', axis=1)
        
        return df_final
    
    @staticmethod
    def system_type():
        '''
        Attributes:
            SystemType (Environment): Requires PYSPARK
        '''
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries
    
    @staticmethod
    def settings() -> dict:
        return {}
    
    def pre_transform_validation(self):
        return True
    
    def post_transform_validation(self):
        return True
