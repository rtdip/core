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
from ._time_series_query_builder import _query_builder


def get(connection: object, parameters_dict: dict) -> pd.DataFrame:
    """
    An RTDIP interpolation at time function which works out the linear interpolation at a specific time based on the points before and after.

    This function requires the user to input a dictionary of parameters. (See Attributes table below.)

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        parameters_dict: A dictionary of parameters (see Attributes table below)

    Attributes:
        business_unit (str): Business unit of the data
        region (str): Region
        asset (str):  Asset
        data_security_level (str): Level of data security
        data_type (str): Type of the data (float, integer, double, string)
        tag_names (str): Name of the tag
        timestamps (list): List of timestamp or timestamps in the format YYY-MM-DDTHH:MM:SS or YYY-MM-DDTHH:MM:SS+zz:zz where %z is the timezone. (Example +00:00 is the UTC timezone)
        window_length (int): Add longer window time in days for the start or end of specified date to cater for edge cases.
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        pivot (bool): Pivot the data on timestamp column with True or do not pivot the data with False
        display_uom (optional bool): Display the unit of measure with True or False. Does not apply to pivoted tables. Defaults to False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows
        case_insensitivity_tag_search (optional bool): Search for tags using case insensitivity with True or case sensitivity with False

    Returns:
        DataFrame: A interpolated at time dataframe.

    !!! warning
        Setting `case_insensitivity_tag_search` to True will result in a longer query time.

    !!! Note
        `display_uom` True will not work in conjunction with `pivot` set to True.
    """
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")

    if isinstance(parameters_dict["timestamps"], list) is False:
        raise ValueError("timestamps must be a list")

    if "pivot" in parameters_dict and "display_uom" in parameters_dict:
        if parameters_dict["pivot"] is True and parameters_dict["display_uom"] is True:
            raise ValueError("pivot True and display_uom True cannot be used together")

    try:
        query = _query_builder(parameters_dict, "interpolation_at_time")

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
        logging.exception("error with interpolation at time function")
        raise e
