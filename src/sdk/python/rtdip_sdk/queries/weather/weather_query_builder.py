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
from ...connectors.connection_interface import ConnectionInterface
from . import (
    raw,
    latest,
)
from pandas import DataFrame


class WeatherQueryBuilder:
    """
    A builder for developing RTDIP forecast queries using any delta table

    """

    parameters: dict
    connection: ConnectionInterface
    close_connection: bool
    data_source: str
    tagname_column: str
    timestamp_column: str
    status_column: str
    value_column: str

    def connect(self, connection: ConnectionInterface):
        """
        Specifies the connection to be used for the query

        Args:
            connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        """
        self.connection = connection
        return self

    def source(
        self,
        source: str,
        tagname_column: str = "TagName",
        timestamp_column: str = "EventTime",
        forecast_run_timestamp_column: str = "EnqueuedTime",
        status_column: Union[str, None] = "Status",
        value_column: str = "Value",
    ):
        """
        Specifies the source of the query

        Args:
            source (str): Source of the query can be a Unity Catalog table, Hive metastore table or path
            tagname_column (optional str): The column name in the source that contains the tagnames or series
            timestamp_column (optional str): The timestamp column name in the source
            forecast_run_timestamp_column (optional str): The forecast run timestamp column name in the source
            status_column (optional str): The status column name in the source indicating `Good` or `Bad`. If this is not available, specify `None`
            value_column (optional str): The value column name in the source which is normally a float or string value for the time series event
        """
        self.data_source = "`.`".join(source.split("."))
        self.tagname_column = tagname_column
        self.timestamp_column = timestamp_column
        self.forecast_run_timestamp_column = forecast_run_timestamp_column
        self.status_column = status_column
        self.value_column = value_column
        return self

    def raw_point(
        self,
        start_date: str,
        end_date: str,
        forecast_run_start_date: str,
        forecast_run_end_date: str,
        lat: float,
        lon: float,
        limit: int = None,
        measurement: str = None,
    ) -> DataFrame:
        """
        A function to return back raw data for a point.

        **Example:**
        ```python
        from rtdip_sdk.queries.weather.weather_query_builder import (
            WeatherQueryBuilder,
        )
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
                WeatherQueryBuilder()
                .connect(connection)
                .source("example.forecast.table")
                .raw_point(
                    start_date="2021-01-01",
                    end_date="2021-01-02",
                    forecast_run_start_date="2021-01-01",
                    forecast_run_end_date="2021-01-02",
                    lat=0.1,
                    lon=0.1,
                )
            )

        print(data)
        ```

        Args:
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            forecast_run_start_date (str): Start date of the forecast run (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            forecast_run_end_date (str): End date of the forecast run (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            lat (float): latitude
            lon (float): longitude
            limit (optional int): The number of rows to be returned
            measurement (optional str): Measurement type

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "source": self.data_source,
            "start_date": start_date,
            "end_date": end_date,
            "forecast_run_start_date": forecast_run_start_date,
            "forecast_run_end_date": forecast_run_end_date,
            "timestamp_column": self.timestamp_column,
            "forecast_run_timestamp_column": self.forecast_run_timestamp_column,
            "lat": lat,
            "lon": lon,
            "limit": limit,
            "measurement": measurement,
            "supress_warning": True,
        }

        return raw.get_point(self.connection, raw_parameters)

    def latest_point(
        self,
        lat: float,
        lon: float,
        limit: int = None,
        measurement: str = None,
    ) -> DataFrame:
        """
        A function to return back the latest data for a point.

        **Example:**
        ```python
        from rtdip_sdk.queries.weather.weather_query_builder import (
            WeatherQueryBuilder,
        )
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
                WeatherQueryBuilder()
                .connect(connection)
                .source("example.forecast.table")
                .latest_point(
                    lat=0.1,
                    lon=0.1,
                    )
                )

        print(data)
        ```

        Args:
            lat (float): latitude
            lon (float): longitude
            limit (optional int): The number of rows to be returned
            measurement (optional str): Measurement type

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "source": self.data_source,
            "lat": lat,
            "lon": lon,
            "limit": limit,
            "measurement": measurement,
            "supress_warning": True,
        }

        return latest.get_point(self.connection, raw_parameters)

    def raw_grid(  # NOSONAR
        self,  # NOSONAR
        start_date: str,
        end_date: str,
        forecast_run_start_date: str,
        forecast_run_end_date: str,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
        limit: int = None,  # NOSONAR
        measurement: str = None,
    ) -> DataFrame:
        """
        A function to return back raw data for a grid.

        **Example:**
        ```python
        from rtdip_sdk.queries.weather.weather_query_builder import (
            WeatherQueryBuilder,
        )
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
                WeatherQueryBuilder()
                .connect(connection)
                .source("example.forecast.table")
                .raw_grid(
                    start_date="2021-01-01",
                    end_date="2021-01-02",
                    forecast_run_start_date="2021-01-01",
                    forecast_run_end_date="2021-01-02",
                    min_lat=0.1,
                    max_lat=0.1,
                    min_lon=0.1,
                    max_lon=0.1,
                )
            )

        print(data)
        ```

        Args:
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            forecast_run_start_date (str): Start date of the forecast run (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            forecast_run_end_date (str): End date of the forecast run (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            min_lat (float): Min latitude
            min_lon (float): Min longitude
            max_lat (float): Max latitude
            max_lon (float): Max longitude
            limit (optional int): The number of rows to be returned
            measurement (optional str): Measurement type

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "source": self.data_source,
            "start_date": start_date,
            "end_date": end_date,
            "forecast_run_start_date": forecast_run_start_date,
            "forecast_run_end_date": forecast_run_end_date,
            "timestamp_column": self.timestamp_column,
            "forecast_run_timestamp_column": self.forecast_run_timestamp_column,
            "min_lat": min_lat,
            "min_lon": min_lon,
            "max_lat": max_lat,
            "max_lon": max_lon,
            "limit": limit,
            "measurement": measurement,
            "supress_warning": True,
        }

        return raw.get_grid(self.connection, raw_parameters)

    def latest_grid(
        self,
        min_lat: float,
        min_lon: float,
        max_lat: float,
        max_lon: float,
        limit: int = None,
        measurement: str = None,
    ) -> DataFrame:
        """
        A function to return back the latest data for a grid.

        **Example:**
        ```python
        from rtdip_sdk.queries.weather.weather_query_builder import (
            WeatherQueryBuilder,
        )
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
                WeatherQueryBuilder()
                .connect(connection)
                .source("example.forecast.table")
                .latest_grid(
                    min_lat=0.1,
                    max_lat=0.1,
                    min_lon=0.1,
                    max_lon=0.1,
                )
            )

        print(data)
        ```

        Args:
            min_lat (float): Min latitude
            min_lon (float): Min longitude
            max_lat (float): Max latitude
            max_lon (float): Max longitude
            limit (optional int): The number of rows to be returned
            measurement (optional str): Measurement type

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "source": self.data_source,
            "min_lat": min_lat,
            "min_lon": min_lon,
            "max_lat": max_lat,
            "max_lon": max_lon,
            "limit": limit,
            "measurement": measurement,
            "supress_warning": True,
        }

        return latest.get_grid(self.connection, raw_parameters)
