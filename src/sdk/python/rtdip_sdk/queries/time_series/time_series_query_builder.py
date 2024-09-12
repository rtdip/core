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
    plot,
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
    metadata_source: str
    metadata_tagname_column: str
    metadata_uom_column: str

    def __init__(self):
        self.metadata_source = None
        self.metadata_tagname_column = None
        self.metadata_uom_column = None

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
                source="{tablename_or_path}"
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

    def m_source(
        self,
        metadata_source: str,
        metadata_tagname_column: str = "TagName",
        metadata_uom_column: str = "UoM",
    ):
        """
        Specifies the Metadata source of the query. This is only required if display_uom is set to True or Step is set to "metadata". Otherwise, it is optional.

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
                source="{tablename_or_path}"
            )
            .m_source(
                metadata_source="{metadata_table_or_path}"
                metadata_tagname_column="TagName",
                metadata_uom_column="UoM")
        )

        ```

        Args:
            metadata_source (str): Source of the query can be a Unity Catalog table, Hive metastore table or path
            metadata_tagname_column (optional str): The column name in the source that contains the tagnames or series
            metadata_uom_column (optional str): The column name in the source that contains the unit of measure
        """
        self.metadata_source = "`.`".join(metadata_source.split("."))
        self.metadata_tagname_column = metadata_tagname_column
        self.metadata_uom_column = metadata_uom_column
        return self

    def raw(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        include_bad_data: bool = False,
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
            .raw(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
            )
        )

        print(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        raw_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if "display_uom" in raw_parameters and raw_parameters["display_uom"] is True:
            if raw_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

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
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
            .resample(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                time_interval_rate="15",
                time_interval_unit="minute",
                agg_method="first",
            )
        )

        print(data)

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
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of resampled timeseries data.
        """

        resample_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "agg_method": agg_method,
            "pivot": pivot,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if (
            "display_uom" in resample_parameters
            and resample_parameters["display_uom"] is True
        ):
            if resample_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

        return resample.get(self.connection, resample_parameters)

    def plot(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        time_interval_rate: str,
        time_interval_unit: str,
        include_bad_data: bool = False,
        pivot: bool = False,
        display_uom: bool = False,
        limit: int = None,
        offset: int = None,
    ) -> DataFrame:
        """
        A query to plot the source data for a time interval for Min, Max, First, Last and an Exception Value(Status = Bad), if it exists.

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
            .source("{tablename_or_path}")
            .plot(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                time_interval_rate="15",
                time_interval_unit="minute",
            )
        )

        print(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            time_interval_rate (str): The time interval rate (numeric input)
            time_interval_unit (str): The time interval unit (second, minute, day, hour)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            pivot (optional bool): Pivot the data on the timestamp column with True or do not pivot the data with False
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of resampled timeseries data.
        """

        plot_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "include_bad_data": include_bad_data,
            "pivot": pivot,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if "display_uom" in plot_parameters and plot_parameters["display_uom"] is True:
            if plot_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

        return plot.get(self.connection, plot_parameters)

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
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
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

        print(data)

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
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of interpolated timeseries data.
        """
        interpolation_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "agg_method": agg_method,
            "interpolation_method": interpolation_method,
            "pivot": pivot,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if (
            "display_uom" in interpolation_parameters
            and interpolation_parameters["display_uom"] is True
        ):
            if interpolation_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

        return interpolate.get(self.connection, interpolation_parameters)

    def interpolation_at_time(
        self,
        tagname_filter: [str],
        timestamp_filter: [str],
        include_bad_data: bool = False,
        window_length: int = 1,
        pivot: bool = False,
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
            .interpolation_at_time(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                timestamp_filter=["2023-01-01T09:30:00", "2023-01-02T12:00:00"],
            )
        )

        print(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            timestamp_filter (list): List of timestamp or timestamps in the format YYY-MM-DDTHH:MM:SS or YYY-MM-DDTHH:MM:SS+zz:zz where %z is the timezone. (Example +00:00 is the UTC timezone)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            window_length (optional int): Add longer window time in days for the start or end of specified date to cater for edge cases
            pivot (optional bool): Pivot the data on the timestamp column with True or do not pivot the data with False
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of interpolation at time timeseries data
        """
        interpolation_at_time_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": tagname_filter,
            "timestamps": timestamp_filter,
            "include_bad_data": include_bad_data,
            "window_length": window_length,
            "pivot": pivot,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if (
            "display_uom" in interpolation_at_time_parameters
            and interpolation_at_time_parameters["display_uom"] is True
        ):
            if interpolation_at_time_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

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
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
            .time_weighted_average(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
                time_interval_rate="15",
                time_interval_unit="minute",
                step="true",
            )
        )

        print(data)

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
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of time weighted averages timeseries data
        """
        time_weighted_average_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
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
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if (
            "display_uom" in time_weighted_average_parameters
            and time_weighted_average_parameters["display_uom"] is True
        ):
            if time_weighted_average_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

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
            .source("{tablename_or_path}")
            .metadata(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
            )
        )

        print(data)

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
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
            .latest(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
            )
        )

        print(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of events latest_values
        """
        latest_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": [] if tagname_filter is None else tagname_filter,
            "tagname_column": self.tagname_column,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if (
            "display_uom" in latest_parameters
            and latest_parameters["display_uom"] is True
        ):
            if latest_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

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
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
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

        print(data)

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
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe containing the circular averages
        """
        circular_average_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "pivot": pivot,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if (
            "display_uom" in circular_average_parameters
            and circular_average_parameters["display_uom"] is True
        ):
            if circular_average_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

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
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
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

        print(data)

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
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe containing the circular standard deviations
        """
        circular_stdev_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "pivot": pivot,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if (
            "display_uom" in circular_stdev_parameters
            and circular_stdev_parameters["display_uom"] is True
        ):
            if circular_stdev_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

        return circular_standard_deviation.get(
            self.connection, circular_stdev_parameters
        )

    def summary(
        self,
        tagname_filter: [str],
        start_date: str,
        end_date: str,
        include_bad_data: bool = False,
        display_uom: bool = False,
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
            .source("{tablename_or_path}")
            .summary(
                tagname_filter=["{tag_name_1}", "{tag_name_2}"],
                start_date="2023-01-01",
                end_date="2023-01-31",
            )
        )

        print(data)

        ```

        Args:
            tagname_filter (list str): List of tagnames to filter on the source
            start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
            include_bad_data (optional bool): Include "Bad" data points with True or remove "Bad" data points with False
            display_uom (optional bool): Display the unit of measure with True or False. Defaults to False. If True, metadata_source must be populated
            limit (optional int): The number of rows to be returned
            offset (optional int): The number of rows to skip before returning rows

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        summary_parameters = {
            "source": self.data_source,
            "metadata_source": self.metadata_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "display_uom": display_uom,
            "limit": limit,
            "offset": offset,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column,
            "metadata_tagname_column": self.metadata_tagname_column,
            "metadata_uom_column": self.metadata_uom_column,
            "supress_warning": True,
        }

        if (
            "display_uom" in summary_parameters
            and summary_parameters["display_uom"] is True
        ):
            if summary_parameters["metadata_source"] is None:
                raise ValueError(
                    "display_uom True requires metadata_source to be populated"
                )

        return summary.get(self.connection, summary_parameters)
