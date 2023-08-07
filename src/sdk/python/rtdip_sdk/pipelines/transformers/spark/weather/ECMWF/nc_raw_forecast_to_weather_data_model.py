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

from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, explode, to_timestamp, when, lit, coalesce
from pyspark.sql.types import ArrayType, StringType

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.weather_ecmwf import RTDIP_STRING_WEATHER_DATA_MODEL, RTDIP_FLOAT_WEATHER_DATA_MODEL 


# -*- coding: utf-8 -*-
"""Extract data from the ecmwf mars files in nc format."""

__author__ = ["ciaran-g", "Amber-Rigg"]


class ExtractBase:
    """
    Base class for extracting data downloaded in nc format from ECMWF.
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

    def raw(self, variables: list, method: str = "nearest") -> pd.DataFrame:
        """Extract raw data from stored nc filed downloaded via ecmwf mars.

        Parameters
        ----------
        variables : list
            variable names of raw tags to be extracted from the nc files
        method : str, optional
            method used to match latitude/longitude in xarray using .sel(), by default
            "nearest"

        Returns
        -------
        pd.DataFrame
            Raw data extracted with lat, lon, run_time, target_time as a pd.multiindex
            and variables as columns.
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

    def process(
        self,
        add_lead_time: bool = True,
        upsample: pd.Timedelta = None,
        upsample_method="linear",
        decumulate_cols: list = None,
        convert_wind: bool = True,
        drop_wind_uv: bool = True,
        convert_dtypes: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """Extract raw data from stored nc filed downloaded via ecmwf mars.

        There are options to decumulate variables, upsample the data, convert
        wind vectors to speed and direction, add lead times in hours, and convert data
        dtypes.

        Note that the order of processing is
            - decumulate -> upsample -> convert_wind -> add lead time -> convert dtypes


        Parameters
        ----------
        add_lead_time : bool, optional
            Add lead time as a column to the resulting dataframe? by default True.
        upsample : pd.Timedelta, optional
            Upsample the data temporally to a desired frequency? by default None
            which means no upsampling.
        upsample_method : str, optional
            If upsample is true the method used for interpolation, by default "linear".
        decumulate_cols : list, optional
            Column names to de-cumulate, e.g. surface solar radiation downwards (ssrd)
            or total precipitation (tp) which are stored in ecmwf as cumulative variables,
            by default None.
        convert_wind : bool, optional
            convert u/v wind vectors to meterological wind speed and direction? by
            default True.
        drop_wind_uv : bool, optional
            if convert_wind is true drop the u/v components from the result? by default
            True.
        convert_dtypes : bool, optional
            Convert columns type using df.convert_dtypes()? by default False.
        `**kwargs` : Keyword arguments to pass on to the .raw() function, e.g. variables.

        Returns
        -------
        pd.DataFrame
            Processed data extracted with lat, lon, run_time, target_time as a
            pd.multiindex and variables as columns.
        """

        # grab point data
        df = self.raw(**kwargs)

        if decumulate_cols is not None:
            df[decumulate_cols] = _decumulate_df(
                df, cols=decumulate_cols, levels=["lat", "lon", "run_time"]
            )

        # this should be done before converting to wind speeds etc
        if upsample is not None:
            df = _upsample(df, freq=upsample, method=upsample_method)

        if convert_wind:
            u_tags = ["u10", "u100", "u200"]
            v_tags = ["v10", "v100", "v200"]
            df_wswd = _convert_uv(df, u_tags=u_tags, v_tags=v_tags)
            df = pd.concat([df, df_wswd], axis=1)
            # drop u and v
            if drop_wind_uv:
                for i in range(len(u_tags)):
                    if np.all([i in df.columns for i in [u_tags[i], v_tags[i]]]):
                        df = df.drop([u_tags[i], v_tags[i]], axis=1)

        if add_lead_time:
            df["lead_time"] = _lead_time_h(df)

        if convert_dtypes:
            df = df.convert_dtypes()

        df = df.sort_index(axis=1)

        return df

class ExtractGrid(ExtractBase):
    """Extract a grid from a local .nc file downloaded from ecmwf via mars

    Parameters
    ----------
    lat_min : float
        Minimum latitude of grid to extract
    lat_max : float
        Maximum latitude of grid to extract
    lon_min : float
        Minimum longitude of grid to extract
    lon_max : float
        Maximum longitude of grid to extract
    grid_step: float
        The grid length to use to define the grid, e.g. 0.1.
    load_path : str
        Path to local directory with nc files downloaded in format "yyyy-mm-dd_HH.nc"
    date_start : str
        Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
    date_end : str
        End date of extraction in "YYYY-MM-DD HH:MM:SS" format
    run_frequency : str
        Frequency format of runs to download, e.g. "H"
    run_interval : str
        Interval of runs. A run_frequency of "H" and run_interval of "12" will
        extract the data of the 00 and 12 run for each day requested.
    utc : bool, optional
        Add utc to the datetime indexes? Defaults to True.
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

        Parameters
        ----------
        fun_dict : dict
            Dictionary containing keys of the column names and values are a list of
            the names of or functions to apply. See pd.DataFrame.aggregate()
        `**kwargs` : Keyword arguments to pass on to the .process() and .raw()
            functions, e.g. variables, decumulate_cols, etc.

        Returns
        -------
        pd.DataFrame
            Processed and aggregated data extracted with lat, lon, run_time, target_time
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


class ExtractCenteredSquare(ExtractGrid):
    """
    Extract a square grid around a central lat/lon point from a local .nc file
    downloaded from ecmwf via mars.

    Parameters
    ----------
    lat_center : float
        Center latitude of square grid to extract
    lon_center : float
        Center longitude of square grid to extract
    grid_step: float
        The grid length to use to define the grid, e.g. 0.1.
    square_with: int
        The length of the square grid to extract, units are number of points.
    load_path : str
        Path to local directory with nc files downloaded in format "yyyy-mm-dd_HH.nc"
    date_start : str
        Start date of extraction in "YYYY-MM-DD HH:MM:SS" format
    date_end : str
        End date of extraction in "YYYY-MM-DD HH:MM:SS" format
    run_frequency : str
        Frequency format of runs to download, e.g. "H"
    run_interval : str
        Interval of runs. A run_frequency of "H" and run_interval of "12" will
        extract the data of the 00 and 12 run for each day requested.
    utc : bool, optional
        Add utc to the datetime indexes? Defaults to True.
    """

    def __init__(
        self,
        lat_center: float,
        lon_center: float,
        grid_step: float,
        square_width: int,
        load_path: str,
        date_start: str,
        date_end: str,
        run_interval: str,
        run_frequency: str,
        utc: bool = True,
    ):
        # hmm careful with floating points, this seems to work ok...
        if square_width % 2 == 0:
            raise ValueError(
                """"square_with should be an odd integer to ensure the square is centered
                around a lat/lon of interest."""
            )

        lat_min = lat_center - grid_step * (square_width / 2) + (grid_step / 2)
        lat_max = lat_center + grid_step * (square_width / 2) - (grid_step / 2)
        lon_min = lon_center - grid_step * (square_width / 2) + (grid_step / 2)
        lon_max = lon_center + grid_step * (square_width / 2) - (grid_step / 2)

        super(ExtractCenteredSquare, self).__init__(
            lat_min=lat_min,
            lat_max=lat_max,
            lon_min=lon_min,
            lon_max=lon_max,
            grid_step=grid_step,
            load_path=load_path,
            date_start=date_start,
            date_end=date_end,
            run_interval=run_interval,
            run_frequency=run_frequency,
            utc=utc,
        )


def _decumulate_df(df, cols, levels=["lat", "lon", "run_time"]):
    grp = df[cols].groupby(level=levels)
    return grp.diff()


def _lead_time_h(df, run_col="run_time", time_col="target_time"):
    lt = (
        df.index.get_level_values(time_col) - df.index.get_level_values(run_col)
    ).total_seconds() / (60 * 60)
    return lt


def _upsample(df, freq, method, levels=["lat", "lon", "run_time"]):
    df = df.groupby(level=levels)
    df = df.apply(
        lambda x: x.droplevel(levels).resample(freq).interpolate(method=method)
    )
    return df


def convert_ws_tag_names(x: list):
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


def _convert_uv(
    df,
    u_tags=["u10", "u100", "u200"],
    v_tags=["v10", "v100", "v200"],
    height=[10, 100, 200],
):
    df_out = pd.DataFrame(index=df.index)
    for i in range(len(u_tags)):
        if np.all([i in df.columns for i in [u_tags[i], v_tags[i]]]):
            # convert to wind speed
            df_out["wind_spd_" + str(height[i])] = (
                df[u_tags[i]] ** 2 + df[v_tags[i]] ** 2
            ) ** (1 / 2)

            # convert to meteorological wind dir deg [0, 360]
            # https://confluence.ecmwf.int/pages/viewpage.action?pageId=133262398
            # also the order of the terms of atan2 is switched round compared to excel
            df_out["wind_dir_" + str(height[i])] = (
                (180 / np.pi) * np.arctan2(df[u_tags[i]], df[v_tags[i]]) + 180
            ) % 360

    return df_out




class Transformer(TransformerInterface):
    '''
    Converts a Spark Dataframe column containing a json string created by OPC Publisher to the Process Control Data Model

    Args:
        data (DataFrame): Dataframe containing the column with Json OPC UA data
        source_column_name (str): Spark Dataframe column containing the OPC Publisher Json OPC UA data
        multiple_rows_per_message (optional bool): Each Dataframe Row contains an array of/multiple OPC UA messages. The list of Json will be exploded into rows in the Dataframe.
        status_null_value (optional str): If populated, will replace null values in the Status column with the specified value.
        timestamp_formats (optional list[str]): Specifies the timestamp formats to be used for converting the timestamp string to a Timestamp Type. For more information on formats, refer to this [documentation.](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
        filter (optional str): Enables providing a filter to the data which can be required in certain scenarios. For example, it would be possible to filter on IoT Hub Device Id and Module by providing a filter in SQL format such as `systemProperties.iothub-connection-device-id = "<Device Id>" AND systemProperties.iothub-connection-module-id = "<Module>"`
    '''
    data: DataFrame
    source_column_name: str
    multiple_rows_per_message: bool
    tagname_field: str
    status_null_value: str
    change_type_value: str
    timestamp_formats: list
    filter: str

    def __init__(self, data: DataFrame, source_column_name: str, multiple_rows_per_message: bool = True, tagname_field: str = "DisplayName", status_null_value: str = None, change_type_value: str = "insert", timestamp_formats: list = ["yyyy-MM-dd'T'HH:mm:ss.SSSX", "yyyy-MM-dd'T'HH:mm:ssX"], filter: str = None) -> None: # NOSONAR
        self.data = data
        self.source_column_name = source_column_name
        self.multiple_rows_per_message = multiple_rows_per_message
        self.tagname_field = tagname_field
        self.status_null_value = status_null_value
        self.change_type_value = change_type_value
        self.timestamp_formats = timestamp_formats
        self.filter = filter

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

    def transform(self) -> DataFrame:
        '''
        Returns:
            DataFrame: A dataframe with the specified column converted to PCDM
        '''
        data_conn = ExtractGrid(
        lat_max=54.9,
        lat_min=54.6,
        lon_max=6.9,
        lon_min=6.6,
        grid_step=0.1,
        load_path="../data/ecmwf/oper/fc/sfc/europe/",
        date_start="2020-10-01",
        date_end="2020-10-02 12:00:00",
        run_interval="12",
        run_frequency="H",
        utc=True
        )

        df = data_conn.raw(
        variables=["ssrd", "10u", "10v", '100u', '100v'],
        method="nearest",
        )

        df.groupby(level=['lat', 'lon', 'run_time']).size()

        df.index.droplevel([-1, -2]).unique()

        df = data_conn.aggregate(
            fun_dict={'ssrd': ['mean'], 'wind_spd_10': ['mean', 'max', 'min']},
    decumulate_cols=['ssrd'],
    upsample=pd.Timedelta(30, 'minutes'),
    upsample_method='linear',
    variables=["ssrd", "10u", "10v"],
            method="nearest",
        )   


        return df.select("TagName", 
                        "Longitude", 
                        "Latitude", 
                        "Longitude", 
                        'EventDate', 
                        'EventTime', 
                        'Source', 
                        'Status', 
                        'Value', 
                        'EnqueuedTime',
                        'Latest'
                        )