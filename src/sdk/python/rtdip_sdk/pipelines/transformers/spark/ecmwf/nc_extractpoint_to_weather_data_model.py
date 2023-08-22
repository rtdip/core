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

import xarray as xr

from .nc_extractbase_to_weather_data_model import ECMWFExtractBaseToWeatherDataModel


class ECMWFExtractPointToWeatherDataModel(ECMWFExtractBaseToWeatherDataModel):
    """
    Extract a single point from a local .nc file downloaded from ECMWF via MARS

    Args:
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

        super(ECMWFExtractPointToWeatherDataModel, self).__init__(
            lat=lat_xr,
            lon=lon_xr,
            load_path=load_path,
            date_start=date_start,
            date_end=date_end,
            run_interval=run_interval,
            run_frequency=run_frequency,
            utc=utc,
        )
