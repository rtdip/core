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
    A function that receives a dataframe of raw tag data and performs a time weighted averages, returning the results.

    This function requires the input of a pandas dataframe acquired via the rtdip.functions.raw() method and the user to input a dictionary of parameters. (See Attributes table below)

    Pi data points will either have step enabled (True) or step disabled (False). You can specify whether you want step to be fetched by "Pi" or you can set the step parameter to True/False in the dictionary below.

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, or deprecated TURBODBC SQL Connect)
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
        window_length (int): Fixed boundary-search window in days (0-366). When omitted, corrected linear modes expand through 7, 30, 180, and 366 days; legacy modes retain the 1-day window.
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        step (str): "true" uses legacy step integration; "false" uses corrected linear interpolation. Explicit "metadata" uses corrected interpolation for tags with false, missing, or null Step metadata. Omitting step retains legacy metadata behavior.
        display_uom (optional bool): Display the unit of measure with True or False. Does not apply to pivoted tables. Defaults to False
        pivot (bool): Pivot the data on timestamp column with True or do not pivot the data with False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows
        case_insensitivity_tag_search (optional bool): Search for tags using case insensitivity with True or case sensitivity with False

    Returns:
        DataFrame: A dataframe containing the time weighted averages.

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

    parameters_dict.setdefault("step_was_provided", "step" in parameters_dict)
    parameters_dict.setdefault(
        "window_length_was_provided", "window_length" in parameters_dict
    )
    parameters_dict.setdefault("window_length", 1)
    if not 0 <= parameters_dict["window_length"] <= 366:
        raise ValueError("window_length must be between 0 and 366 days")
    if "step" not in parameters_dict:  # default step to metadata if not provided
        parameters_dict["step"] = "metadata"

    step_was_provided = parameters_dict.get(
        "step_was_provided", "step" in parameters_dict
    )
    adaptive_linear_search = (
        parameters_dict["step"] == "false"
        or (parameters_dict["step"] == "metadata" and step_was_provided)
    ) and not parameters_dict["window_length_was_provided"]
    search_windows = [7, 30, 180, 366] if adaptive_linear_search else [
        parameters_dict["window_length"]
    ]

    try:
        try:
            df = None
            for window_length in search_windows:
                query_parameters = parameters_dict.copy()
                query_parameters["adaptive_linear_search"] = adaptive_linear_search
                if adaptive_linear_search:
                    query_parameters["_linear_window_length"] = window_length
                query = _query_builder(query_parameters, "time_weighted_average")
                cursor = connection.cursor()
                try:
                    cursor.execute(query)
                    df = cursor.fetch_all()
                finally:
                    cursor.close()

                if (
                    not adaptive_linear_search
                    or "_TwaBoundarySearchIncomplete" not in df.columns
                    or not pd.to_numeric(
                        df["_TwaBoundarySearchIncomplete"], errors="coerce"
                    )
                    .fillna(0)
                    .gt(0)
                    .any()
                    or window_length == search_windows[-1]
                ):
                    break

            if "_TwaBoundarySearchIncomplete" in df.columns:
                df = df.drop(columns=["_TwaBoundarySearchIncomplete"])

            connection.close()
            return df
        except Exception as e:
            logging.exception("error returning dataframe")
            raise e

    except Exception as e:
        logging.exception("error with time weighted average function")
        raise e
