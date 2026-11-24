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
from ._boundary_search import extract_boundary_search_status
from ._time_series_query_builder import _query_builder


def get(connection: object, parameters_dict: dict) -> pd.DataFrame:
    """
    An RTDIP interpolation function that is intertwined with the RTDIP Resampling function.

    The Interpolation function will forward fill, backward fill or linearly interpolate the resampled data depending users specified interpolation method.

    This function requires the user to input a dictionary of parameters. (See Attributes table below.)

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, or deprecated TURBODBC SQL Connect)
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
        time_interval_rate (str): The time interval rate (numeric input)
        time_interval_unit (str): The time interval unit (second, minute, day, hour)
        step (optional str): Omit or pass null to retain the current linear interpolation. Explicit true uses step-hold interpolation, false uses linear interpolation, and metadata selects the method from each tag's Step metadata.
        window_length (optional int): Fixed boundary-search window in days (0-366). When omitted, explicit false/metadata modes expand through 7, 30, 180, and 366 days. Omitted/null/true retain the existing time-gradient buffer.
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        pivot (bool): Pivot the data on timestamp column with True or do not pivot the data with False
        display_uom (optional bool): Display the unit of measure with True or False. Does not apply to pivoted tables. Defaults to False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows
        sort (optional bool): Sort the data in ascending order by the TagName and Timestamp columns or, if pivot is True, by the Timestamp column
        case_insensitivity_tag_search (optional bool): Search for tags using case insensitivity with True or case sensitivity with False

    Returns:
        DataFrame: A resampled and interpolated dataframe.

    !!! warning
        **Time Gradient Buffering**

        The interpolation query automatically expands the requested date range with a time-based buffer to ensure accurate interpolation at the boundaries. The buffer size depends on the `time_interval_unit`:

        - **minute**: ±5 minutes
        - **second**: ±60 seconds
        - **hour**: ±1 hour
        - **day**: ±1 day

        **Example**: If you request data from 2024-01-01 00:00:00 to 2024-01-01 23:59:59 with minute intervals, the query will fetch data from 2023-12-31 23:55:00 to 2024-01-02 00:04:59. Only data within your requested range will be returned, but the buffer ensures interpolated values at your boundaries are accurate.

        **Important**: In cases where data collection is infrequent (e.g., sparse sensor data), the default buffer boundaries may not cover sufficient historical/future data points for accurate interpolation. If you encounter interpolation gaps or inaccurate boundary values, consider expanding your query window to include more data around your requested date range. This ensures sufficient data points exist within the buffer for computing accurate interpolated values.

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

    step = parameters_dict.get("step")
    if isinstance(step, str):
        step = None if step.lower() == "null" else step.lower()
    if step not in (None, "true", "false", "metadata"):
        raise ValueError("step must be one of: true, false, metadata, or null")
    parameters_dict["step"] = step
    parameters_dict.setdefault(
        "window_length_was_provided", "window_length" in parameters_dict
    )
    parameters_dict.setdefault("window_length", 7)
    if not 0 <= parameters_dict["window_length"] <= 366:
        raise ValueError("window_length must be between 0 and 366 days")
    adaptive_search = step in ("false", "metadata") and not parameters_dict[
        "window_length_was_provided"
    ]
    search_windows = [7, 30, 180, 366] if adaptive_search else [
        parameters_dict["window_length"]
    ]

    try:
        try:
            df = None
            for window_length in search_windows:
                query_parameters = parameters_dict.copy()
                query_parameters["adaptive_search"] = adaptive_search
                if adaptive_search:
                    query_parameters["_linear_window_length"] = window_length
                query = _query_builder(query_parameters, "interpolate")
                cursor = connection.cursor()
                try:
                    cursor.execute(query)
                    df = cursor.fetch_all()
                finally:
                    cursor.close()
                df, boundary_search_incomplete = extract_boundary_search_status(
                    df, "_InterpolateBoundarySearchIncomplete"
                )

                if (
                    not adaptive_search
                    or not boundary_search_incomplete
                    or window_length == search_windows[-1]
                ):
                    break

            connection.close()
            return df
        except Exception as e:
            logging.exception("error returning dataframe")
            raise e

    except Exception as e:
        logging.exception("error with interpolate function")
        raise e
