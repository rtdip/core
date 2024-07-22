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
    A function to return the latest event values by querying databricks SQL Warehouse using a connection specified by the user.

    The available connectors by RTDIP are Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect.

    The available authentcation methods are Certificate Authentication, Client Secret Authentication or Default Authentication. See documentation.

    This function requires the user to input a dictionary of parameters. (See Attributes table below)

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        parameters_dict: A dictionary of parameters (see Attributes table below)

    Attributes:
        business_unit (str): Business unit
        region (str): Region
        asset (str): Asset
        data_security_level (str): Level of data security
        tag_names (optional, list): Either pass a list of tagname/tagnames ["tag_1", "tag_2"] or leave the list blank [] or leave the parameter out completely
        display_uom (optional bool): Display the unit of measure with True or False. Does not apply to pivoted tables. Defaults to False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows
        case_insensitivity_tag_search (optional bool): Search for tags using case insensitivity with True or case sensitivity with False

    Returns:
        DataFrame: A dataframe of event latest values.

    !!! warning
        Setting `case_insensitivity_tag_search` to True will result in a longer query time.
    """
    try:
        query = _query_builder(parameters_dict, "latest")

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
        logging.exception("error returning latest function")
        raise e
