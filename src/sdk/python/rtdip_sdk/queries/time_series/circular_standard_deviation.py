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
    A function that receives a dataframe of raw tag data and computes the circular standard deviation for samples assumed to be in the range, returning the results.

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        parameters_dict (dict): A dictionary of parameters (see Attributes table below)

    Attributes:
        business_unit (str): Business unit
        region (str): Region
        asset (str): Asset
        data_security_level (str): Level of data security
        data_type (str): Type of the data (float, integer, double, string)
        tag_names (list): List of tagname or tagnames
        start_date (str): Start date (Either a utc date in the format YYYY-MM-DD or a utc datetime in the format YYYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        end_date (str): End date (Either a utc date in the format YYYY-MM-DD or a utc datetime in the format YYYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        time_interval_rate (str): The time interval rate (numeric input)
        time_interval_unit (str): The time interval unit (second, minute, day, hour)
        lower_bound (int): Lower boundary for the sample range
        upper_bound (int): Upper boundary for the sample range
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        pivot (bool): Pivot the data on timestamp column with True or do not pivot the data with False
        display_uom (optional bool): Display the unit of measure with True or False. Defaults to False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows
        case_insensitivity_tag_search (optional bool): Search for tags using case insensitivity with True or case sensitivity with False

    Returns:
        DataFrame: A dataframe containing the circular standard deviations.

    !!! warning
        Setting `case_insensitivity_tag_search` to True will result in a longer query time.

    !!! Note
        `display_uom` True will not work in conjunction with `pivot` set to True.
    """
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")

    if "pivot" in parameters_dict and "display_uom" in parameters_dict:
        if parameters_dict["pivot"] is True and parameters_dict["display_uom"] is True:
            raise ValueError("pivot True and display_uom True cannot be used together")

    try:
        query = _query_builder(parameters_dict, "circular_standard_deviation")

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
        logging.exception("error with circular standard_deviation function")
        raise e
