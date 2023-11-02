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

from typing import Union
from ..connectors.connection_interface import ConnectionInterface
from .time_series.weather import (
    raw,
    latest,
)
from . import metadata
from pandas import DataFrame


class QueryBuilder:
    """
    A builder for developing RTDIP queries using any delta table
    """

    parameters: dict
    connection: ConnectionInterface

    def connect(self, connection: ConnectionInterface):
        """
        Specifies the connection to be used for the query

        Args:
            connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        """
        self.connection = connection
        return self

    def raw_point(
        self,
        forecast: str,
        region: str,
        data_security_level: str,
        data_type: str,
        start_date: str,
        end_date: str,
        lat: float,
        lon: float,
        source: str = None,
        time_zone: str = None,
        include_bad_data: bool = False,
        limit: int = None,
    ) -> DataFrame:
        """
        A function to return back raw data for a point.

        Args:
            forecast (str): Business unit
            region (str): Region
            data_security_level (str): Level of data security
            data_type (str): Type of the data (float, integer, double, string)
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            lat (float): latitude
            lon (float): longitude
            source (optional str): Source of the data ie ECMWF
            time_zone (str): Timezone of the data
            include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
            limit (optional int): The number of rows to be returned

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "forecast": forecast,
            "region": region,
            "data_security_level": data_security_level,
            "data_type": data_type,
            "start_date": start_date,
            "end_date": end_date,
            "lat": lat,
            "lon": lon,
            "source": source,
            "time_zone": time_zone,
            "include_bad_data": include_bad_data,
            "limit": limit,
        }

        return raw.get_point(self.connection, raw_parameters)

    def latest_point(
        self,
        forecast: str,
        region: str,
        data_security_level: str,
        data_type: str,
        lat: float,
        lon: float,
        source: str = None,
        limit: int = None,
    ) -> DataFrame:
        """
        A function to return back the latest data for a point.

        Args:
            forecast (str): Business unit
            region (str): Region
            data_security_level (str): Level of data security
            data_type (str): Type of the data (float, integer, double, string)
            lat (float): latitude
            lon (float): longitude
            source (optional str): Source of the data ie ECMWF
            limit (optional int): The number of rows to be returned

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "forecast": forecast,
            "region": region,
            "data_security_level": data_security_level,
            "data_type": data_type,
            "lat": lat,
            "lon": lon,
            "source": source,
            "limit": limit,
        }

        return latest.get_point(self.connection, raw_parameters)

    def raw_grid(
        self,
        forecast: str,
        region: str,
        data_security_level: str,
        data_type: str,
        start_date: str,
        end_date: str,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
        source: str = None,
        time_zone: str = None,
        include_bad_data: bool = False,
        limit: int = None,  # NOSONAR
    ) -> DataFrame:
        """
        A function to return back raw data for a point.

        Args:
            forecast (str): Business unit
            region (str): Region
            data_security_level (str): Level of data security
            data_type (str): Type of the data (float, integer, double, string)
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            min_lat (float): Min latitude
            min_lon (float): Min longitude
            max_lat (float): Max latitude
            max_lon (float): Max longitude
            source (optional str): Source of the data ie ECMWF
            time_zone (str): Timezone of the data
            include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
            limit (optional int): The number of rows to be returned

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "forecast": forecast,
            "region": region,
            "data_security_level": data_security_level,
            "data_type": data_type,
            "start_date": start_date,
            "end_date": end_date,
            "min_lat": min_lat,
            "min_lon": min_lon,
            "max_lat": max_lat,
            "max_lon": max_lon,
            "source": source,
            "time_zone": time_zone,
            "include_bad_data": include_bad_data,
            "limit": limit,
        }

        return raw.get_grid(self.connection, raw_parameters)

    def latest_grid(
        self,
        forecast: str,
        region: str,
        data_security_level: str,
        data_type: str,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
        source: str = None,
        limit: int = None,
    ) -> DataFrame:
        """
        A function to return back the latest data for a point.

        Args:
            forecast (str): Business unit
            region (str): Region
            data_security_level (str): Level of data security
            data_type (str): Type of the data (float, integer, double, string)
            min_lat (float): Min latitude
            min_lon (float): Min longitude
            max_lat (float): Max latitude
            max_lon (float): Max longitude
            source (optional str): Source of the data ie ECMWF
            limit (optional int): The number of rows to be returned

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "forecast": forecast,
            "region": region,
            "data_security_level": data_security_level,
            "data_type": data_type,
            "min_lat": min_lat,
            "min_lon": min_lon,
            "max_lat": max_lat,
            "max_lon": max_lon,
            "source": source,
            "limit": limit,
        }

        return latest.get_grid(self.connection, raw_parameters)
