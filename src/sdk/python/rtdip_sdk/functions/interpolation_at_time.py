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
from src.sdk.python.rtdip_sdk.functions._query_builder import _query_builder

def get(connection: object, parameters_dict: dict) -> pd.DataFrame:
    '''
    An RTDIP interpolation function at time.
    
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
        tag_name (str): Name of the tag
        start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS)
        end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS)
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False

    Returns:
        DataFrame: A interpolated at time dataframe.
    '''
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")

    try:
        query = _query_builder(parameters_dict, metadata=False, interpolation_at_time=True)

        try:
            cursor = connection.cursor()
            cursor.execute(query)
            df = cursor.fetch_all()
            cursor.close()
            return df
        except Exception as e:
            logging.exception('error returning dataframe')
            raise e

    except Exception as e:
        logging.exception('error with interpolation at time function')
        raise e
    
from src.sdk.python.rtdip_sdk.authentication.authenticate import DefaultAuth
from src.sdk.python.rtdip_sdk.odbc.db_sql_connector import DatabricksSQLConnection

#testing 
auth = DefaultAuth(exclude_cli_credential=True,exclude_powershell_credential=True,exclude_shared_token_cache_credential=True,logging_enable=True,exclude_visual_studio_code_credential=True).authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("adb-3073476248944970.10.azuredatabricks.net", "/sql/1.0/warehouses/f714de9e353afa66", token)

dict = {
    "tag_names": ["Tag1", "Tag2"], 
    "start_date": "2023-04-30T13:57:30",
    "end_date": "2023-04-30T14:05:00",
}
x = get(connection, dict)
print(x)