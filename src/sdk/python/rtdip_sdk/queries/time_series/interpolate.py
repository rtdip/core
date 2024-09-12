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
    An RTDIP interpolation function that is intertwined with the RTDIP Resampling function.

    The Interpolation function will forward fill, backward fill or linearly interpolate the resampled data depending users specified interpolation method.

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
        tag_names (list): List of tagname or tagnames ["tag_1", "tag_2"]
        start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        sample_rate (int): (deprecated) Please use time_interval_rate instead. See below.
        sample_unit (str): (deprecated) Please use time_interval_unit instead. See below.
        time_interval_rate (str): The time interval rate (numeric input)
        time_interval_unit (str): The time interval unit (second, minute, day, hour)
        agg_method (str): Aggregation Method (first, last, avg, min, max)
        interpolation_method (str): Interpolation method (forward_fill, backward_fill, linear)
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        pivot (bool): Pivot the data on timestamp column with True or do not pivot the data with False
        display_uom (optional bool): Display the unit of measure with True or False. Does not apply to pivoted tables. Defaults to False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows
        case_insensitivity_tag_search (optional bool): Search for tags using case insensitivity with True or case sensitivity with False

    Returns:
        DataFrame: A resampled and interpolated dataframe.

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

    if "sample_rate" in parameters_dict:
        logging.warning(
            "Parameter sample_rate is deprecated and will be removed in v1.0.0. Please use time_interval_rate instead."
        )
        parameters_dict["time_interval_rate"] = parameters_dict["sample_rate"]

    if "sample_unit" in parameters_dict:
        logging.warning(
            "Parameter sample_unit is deprecated and will be removed in v1.0.0. Please use time_interval_unit instead."
        )
        parameters_dict["time_interval_unit"] = parameters_dict["sample_unit"]

    try:
        query = _query_builder(parameters_dict, "interpolate")

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
        logging.exception("error with interpolate function")
        raise e
