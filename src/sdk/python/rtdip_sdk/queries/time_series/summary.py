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
    A function to return back a summary of statistics (Avg, Min, Max, Count, StDev, Sum, Variance) by querying databricks SQL Warehouse using a connection specified by the user.

    The available connectors by RTDIP are Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect.

    The available authentication methods are Certificate Authentication, Client Secret Authentication or Default Authentication. See documentation.

    This function requires the user to input a dictionary of parameters. (See Attributes table below)

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        parameters_dict: A dictionary of parameters (see Attributes table below)

    Attributes:
        business_unit (str): Business unit
        region (str): Region
        asset (str): Asset
        data_security_level (str): Level of data security
        data_type (str): Type of the data (float, integer, double, string)
        tag_names (list): List of tagname or tagnames ["tag_1", "tag_2"]
        start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        display_uom (optional bool): Display the unit of measure with True or False. Does not apply to pivoted tables. Defaults to False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows
        case_insensitivity_tag_search (optional bool): Search for tags using case insensitivity with True or case sensitivity with False

    Returns:
        DataFrame: A dataframe of summary statistics.

    !!! warning
        Setting `case_insensitivity_tag_search` to True will result in a longer query time.
    """
    try:
        query = _query_builder(parameters_dict, "summary")

        try:
            cursor = connection.cursor()
            cursor.execute(query)
            df = cursor.fetch_all()
            cursor.close()
            connection.close()
            return df
        except Exception as e:
            logging.exception("error returning summary dataframe")
            raise e

    except Exception as e:
        logging.exception("error with summary function")
        raise e
