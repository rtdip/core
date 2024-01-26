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
    resample,
    interpolate,
    interpolation_at_time,
    time_weighted_average,
    circular_average,
    circular_standard_deviation,
    latest,
    summary,
)
from .. import metadata
from pandas import DataFrame


class TimeSeriesQueryBuilder:
    """
    A builder for developing RTDIP queries using any delta table.
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
        Specifies the connection to be used for the query.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        connect = (
            TimeSeriesQueryBuilder()
            .connect(connection)
        )

        ```

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
        status_column: Union[str, None] = "Status",
        value_column: str = "Value",
    ):
        """
        Specifies the source of the query.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        source = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source(
                source="{table_path}"
            )
        )

        ```

        Args:
            source (str): Source of the query can be a Unity Catalog table, Hive metastore table or path
            tagname_column (optional str): The column name in the source that contains the tagnames or series
            timestamp_column (optional str): The timestamp column name in the source
            status_column (optional str): The status column name in the source indicating `Good` or `Bad`. If this is not available, specify `None`
            value_column (optional str): The value column name in the source which is normally a float or string value for the time series event
        """
        self.data_source = "`.`".join(source.split("."))
        self.tagname_column = tagname_column
        self.timestamp_column = timestamp_column
        self.status_column = status_column
        self.value_column = value_column
        return self

    def raw(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        include_bad_data: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A function to return back raw data.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .raw(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "supress_warning": True,
        }
        return raw.get(self.connection, raw_parameters)

    def resample(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        time_interval_rate: str,
        time_interval_unit: str,
        agg_method: str,
        include_bad_data: bool = False,
        pivot: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A query to resample the source data.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .resample(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                time_interval_rate="15",
                time_interval_unit="minute",
                agg_method="first",
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            time_interval_rate (str): The time interval rate (numeric input)
            time_interval_unit (str): The time interval unit (second, minute, day, hour)
            agg_method (str): Aggregation Method (first, last, avg, min, max)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            pivot (optional bool): Pivot the data on the timestamp column with True or do not pivot the data with False
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of resampled timeseries data.
        """

        resample_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "agg_method": agg_method,
            "pivot": pivot,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "supress_warning": True,
        }

        return resample.get(self.connection, resample_parameters)

    def interpolate(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        time_interval_rate: str,
        time_interval_unit: str,
        agg_method: str,
        interpolation_method: str,
        include_bad_data: bool = False,
        pivot: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        The Interpolate function will forward fill, backward fill or linearly interpolate the resampled data depending on the parameters specified.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .interpolate(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                time_interval_rate="15",
                time_interval_unit="minute",
                agg_method="first",
                interpolation_method="forward_fill",
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            time_interval_rate (str): The time interval rate (numeric input)
            time_interval_unit (str): The time interval unit (second, minute, day, hour)
            agg_method (str): Aggregation Method (first, last, avg, min, max)
            interpolation_method (str): Interpolation method (forward_fill, backward_fill, linear)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            pivot (optional bool): Pivot the data on the timestamp column with True or do not pivot the data with False
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of interpolated timeseries data.
        """
        interpolation_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "agg_method": agg_method,
            "interpolation_method": interpolation_method,
            "pivot": pivot,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "supress_warning": True,
        }

        return interpolate.get(self.connection, interpolation_parameters)

    def interpolation_at_time(
        self,
        tagname_filter: [str],
        timestamp_filter: [str],
        include_bad_data: bool = False,
        window_length: int = 1,
        pivot: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A interpolation at time function which works out the linear interpolation at a specific time based on the points before and after.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .interpolation_at_time(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                timestamp_filter=["2023-01-01T09:30:00", "2023-01-02T12:00:00"],
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            timestamp_filter (list): List of timestamp or timestamps in the format YYY-MM-DDTHH:MM:SS or YYY-MM-DDTHH:MM:SS+zz:zz where %z is the timezone. (Example +00:00 is the UTC timezone)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            window_length (optional int): Add longer window time in days for the start or end of specified date to cater for edge cases
            pivot (optional bool): Pivot the data on the timestamp column with True or do not pivot the data with False
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of interpolation at time timeseries data
        """
        interpolation_at_time_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "timestamps": timestamp_filter,
            "include_bad_data": include_bad_data,
            "window_length": window_length,
            "pivot": pivot,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "supress_warning": True,
        }

        return interpolation_at_time.get(
            self.connection, interpolation_at_time_parameters
        )

    def time_weighted_average(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        time_interval_rate: str,
        time_interval_unit: str,
        step: str,
        source_metadata: str = None,
        include_bad_data: bool = False,
        window_length: int = 1,
        pivot: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A function that receives a dataframe of raw tag data and performs a time weighted averages.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .time_weighted_average(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                time_interval_rate="15",
                time_interval_unit="minute",
                step="true",
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            time_interval_rate (str): The time interval rate (numeric input)
            time_interval_unit (str): The time interval unit (second, minute, day, hour)
            step (str): data points with step "enabled" or "disabled". The options for step are "true", "false" or "metadata". "metadata" will retrieve the step value from the metadata table
            source_metadata (optional str): if step is set to "metadata", then this parameter must be populated with the source containing the tagname metadata with a column called "Step"
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            window_length (optional int): Add longer window time in days for the start or end of specified date to cater for edge cases
            pivot (optional bool): Pivot the data on the timestamp column with True or do not pivot the data with False
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of time weighted averages timeseries data
        """
        time_weighted_average_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "step": step,
            "source_metadata": (
                None
                if source_metadata is None
                else "`.`".join(source_metadata.split("."))
            ),
            "window_length": window_length,
            "pivot": pivot,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "supress_warning": True,
        }

        return time_weighted_average.get(
            self.connection, time_weighted_average_parameters
        )

    def metadata(
        self,
        tagname_filter: [str] = None,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A query to retrieve metadata.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .metadata(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of metadata
        """
        metadata_parameters = {
            "source": self.data_source,
            "tag_names": [] if tagname_filter is None else tagname_filter,
            "tagname_column": self.tagname_column,
            "limit": limit,
            "offset": offset,
            "supress_warning": True,
        }

        return metadata.get(self.connection, metadata_parameters)

    def latest(
        self,
        tagname_filter: [str] = None,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A query to retrieve latest event_values.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .latest(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of events latest_values
        """
        latest_parameters = {
            "source": self.data_source,
            "tag_names": [] if tagname_filter is None else tagname_filter,
            "tagname_column": self.tagname_column,
            "limit": limit,
            "offset": offset,
            "supress_warning": True,
        }

        return latest.get(self.connection, latest_parameters)

    def circular_average(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        time_interval_rate: str,
        time_interval_unit: str,
        lower_bound: int,
        upper_bound: int,
        include_bad_data: bool = False,
        pivot: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A function that receives a dataframe of raw tag data and computes the circular mean for samples in a range.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .circular_average(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                time_interval_rate="15",
                time_interval_unit="minute",
                lower_bound="0",
                upper_bound="360",
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            time_interval_rate (str): The time interval rate (numeric input)
            time_interval_unit (str): The time interval unit (second, minute, day, hour)
            lower_bound (int): Lower boundary for the sample range
            upper_bound (int): Upper boundary for the sample range
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            pivot (optional bool): Pivot the data on the timestamp column with True or do not pivot the data with False
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe containing the circular averages
        """
        circular_average_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "pivot": pivot,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "supress_warning": True,
        }

        return circular_average.get(self.connection, circular_average_parameters)

    def circular_standard_deviation(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        time_interval_rate: str,
        time_interval_unit: str,
        lower_bound: int,
        upper_bound: int,
        include_bad_data: bool = False,
        pivot: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A function that receives a dataframe of raw tag data and computes the circular standard deviation for samples assumed to be in the range.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .circular_standard_deviation(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                time_interval_rate="15",
                time_interval_unit="minute",
                lower_bound="0",
                upper_bound="360",
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            time_interval_rate (str): The time interval rate (numeric input)
            time_interval_unit (str): The time interval unit (second, minute, day, hour)
            lower_bound (int): Lower boundary for the sample range
            upper_bound (int): Upper boundary for the sample range
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            pivot (optional bool): Pivot the data on the timestamp column with True or do not pivot the data with False
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe containing the circular standard deviations
        """
        circular_stdev_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "pivot": pivot,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "supress_warning": True,
        }

        return circular_standard_deviation.get(
            self.connection, circular_stdev_parameters
        )

    def summary(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        include_bad_data: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A function to return back a summary of statistics.

        **Example:**
        ```python
        from rtdip_sdk.authentication.azure import DefaultAuth
        from rtdip_sdk.connectors import DatabricksSQLConnection
        from rtdip_sdk.queries import TimeSeriesQueryBuilder

        auth = DefaultAuth().authenticate()
        token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
        connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

        data = (
            TimeSeriesQueryBuilder()
            .connect(connection)
            .source("{table_path}")
            .summary(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
            )
        )

        display(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        summary_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "supress_warning": True,
        }
        return summary.get(self.connection, summary_parameters)
