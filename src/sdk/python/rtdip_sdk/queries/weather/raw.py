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

import logging
import pandas as pd
from ._weather_query_builder import _query_builder


def get_grid(connection: object, parameters_dict: dict) -> pd.DataFrame:
    """
    A function to return back raw data by querying databricks SQL Warehouse using a connection specified by the user.

    This will return the raw weather forecast data for a grid.

    The available connectors by RTDIP are Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect.

    The available authentication methods are Certificate Authentication, Client Secret Authentication or Default Authentication. See documentation.

    This function requires the user to input a dictionary of parameters which are focused on Weather Data. (See Attributes table below)

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        parameters_dict: A dictionary of parameters (see Attributes table below)

    Attributes:
        source (optional str): Source of the data the full table name
        forecast (str): Any specific identifier for forecast
        forecast_type(str): Type of forecast ie weather, solar, power, etc
        region (str): Region
        data_security_level (str): Level of data security
        data_type (str): Type of the data (float, integer, double, string)
        start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        forecast_run_start_date (str): Start date of the forecast run (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        forecast_run_end_date (str): End date of the forecast run (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        timestamp_column (str): The column which contains the the forecast output time. Default "EventTime".
        forecast_run_timestamp_column (str): The column which contains whent the forecast was run. Default "EnqueuedTime".
        max_lat (float): Maximum latitude
        max_lon (float): Maximum longitude
        min_lat (float): Minimum latitude
        min_lon (float): Minimum longitude
        measurement (optional str): Measurement type
        limit (optional int): The number of rows to be returned
    }

    Returns:
        DataFrame: A dataframe of raw weather forecast data.
    """
    try:
        query = _query_builder(parameters_dict, "raw_grid")

        print(query)

        try:
            cursor = connection.cursor()
            cursor.execute(query)
            df = cursor.fetch_all()
            cursor.close()
            connection.close()
            return df
        except Exception as e:
            logging.exception("error returning dataframe")
            raise e

    except Exception as e:
        logging.exception("error with raw function")
        raise e


def get_point(connection: object, parameters_dict: dict) -> pd.DataFrame:
    """
    A function to return back raw data by querying databricks SQL Warehouse using a connection specified by the user.

    This will return the raw weather forecast data for a single point.

    The available connectors by RTDIP are Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect.

    The available authentication methods are Certificate Authentication, Client Secret Authentication or Default Authentication. See documentation.

    This function requires the user to input a dictionary of parameters which are focused on Weather Data. (See Attributes table below)

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        parameters_dict: A dictionary of parameters (see Attributes table below)

    Attributes:
        source (optional str): Source of the data the full table name
        forecast (str): Any specific identifier for forecast
        forecast_type(str): Type of forecast ie weather, solar, power, etc
        region (str): Region
        data_security_level (str): Level of data security
        data_type (str): Type of the data (float, integer, double, string)
        start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        forecast_run_start_date (str): Start date of the forecast run (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        forecast_run_end_date (str): End date of the forecast run (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        timestamp_column (str): The column which contains the the forecast output time. Default "EventTime".
        forecast_run_timestamp_column (str): The column which contains whent the forecast was run. Default "EnqueuedTime.
        lat (float): latitude
        lon (float): longitude
        measurement (optional str): Measurement type
        limit (optional int): The number of rows to be returned
    }

    Returns:
        DataFrame: A dataframe of raw weather forecast data.
    """
    try:
        query = _query_builder(parameters_dict, "raw_point")

        print(query)

        try:
            cursor = connection.cursor()
            cursor.execute(query)
            df = cursor.fetch_all()
            cursor.close()
            connection.close()
            return df
        except Exception as e:
            logging.exception("error returning dataframe")
            raise e

    except Exception as e:
        logging.exception("error with raw function")
        raise e
