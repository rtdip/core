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

import os
import pandas as pd
import numpy as np
import xarray as xr

from ...interfaces import TransformerInterface
from ...._pipeline_utils.models import Libraries, SystemType
from ...._pipeline_utils.weather_ecmwf import (
    RTDIP_STRING_WEATHER_DATA_MODEL,
    RTDIP_FLOAT_WEATHER_DATA_MODEL,
)


class ECMWFExtractBaseToWeatherDataModel(TransformerInterface):
    """
    Base class for extracting forecast data downloaded in .nc format from ECMWF MARS Server.

    Args:
        load_path (str): Path to local directory where the nc files will be stored, in format "yyyy-mm-dd_HH.nc"
        date_start (str): Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
        date_end (str): End date of extraction in "YYYY-MM-DD HH:MM:SS" format
        run_frequency (str):Frequency format of runs to download, e.g. "H"
        run_interval (str): Interval of runs, e.g. a run_frequency of "H" and run_interval of "12" will extract the data of the 00 and 12 run for each day.
        lat (DataArray): Latitude values to extract from nc files
        lon (DataArray): Longitude values to extract from nc files
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

    def pre_transform_validation(self):
        return True

    def post_transform_validation(self):
        return True

    @staticmethod
    def _convert_ws_tag_names(x: list):
        """
        Converts the tag names of wind speed from the format used in the nc files to the format used in the weather data model.

        Args:
            x (list): List of variable names of raw tags to be extracted from the nc files

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
        new_tags = [convert_dict[i] if i in convert_dict.keys() else i for i in x]
        return new_tags

    def transform(
        self, tag_prefix: str, variables: list, method: str = "nearest"
    ) -> pd.DataFrame:
        """Extract raw data from stored nc filed downloaded via ECMWF MARS.

        Args:
            tag_prefix (str): Prefix of the tag names of raw tags to be added to the dataframe
            variables (list): List of variable names of raw tags to be extracted from the nc files
            method (str, optional): The method used to match latitude/longitude in xarray using .sel(), by default "nearest"

        Returns:
            df (pd.DataFrame): Raw data extracted with lat, lon, run_time, target_time as a pd.multiindex and variables as columns.
        """
        df = []
        # e.g. 10u variable is saved as u10 in the file...
        vars_processed = self._convert_ws_tag_names(variables)

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

        if "level" in df.index.names:
            index_names = ["lat", "lon", "level", "run_time", "target_time"]
        else:
            index_names = ["lat", "lon", "run_time", "target_time"]
        df = df.reset_index().set_index(index_names)

        if self.utc:
            df = df.tz_localize("UTC", level="target_time")
            df = df.tz_localize("UTC", level="run_time")

        df = df[~(df.index.duplicated(keep="first"))]
        df = df.sort_index(axis=0)
        df = df.sort_index(axis=1)

        df_new = df.reset_index()

        df_new = df_new.rename(
            columns={
                "lat": "Latitude",
                "lon": "Longitude",
                "run_time": "EnqueuedTime",
                "target_time": "EventTime",
            }
        )

        df_new = (
            df_new.set_index(["Latitude", "Longitude", "EnqueuedTime", "EventTime"])[
                vars_processed
            ]
            .rename_axis("Measure", axis=1)
            .stack()
            .reset_index(name="Value")
        )

        df_new["Source"] = "ECMWF_MARS"
        df_new["Status"] = "Good"
        df_new["Latest"] = True
        df_new["EventDate"] = pd.to_datetime(df_new["EventTime"]).dt.date
        df_new["TagName"] = (
            tag_prefix
            + df_new["Latitude"].astype(str)
            + "_"
            + df_new["Longitude"].astype(str)
            + "_"
            + df_new["Source"]
            + "_"
            + df_new["Measure"]
        )
        df_final = df_new.drop("Measure", axis=1)

        return df_final
