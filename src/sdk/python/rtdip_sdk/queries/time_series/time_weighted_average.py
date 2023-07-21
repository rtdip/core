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
import sys
sys.path.insert(0, '.')
from src.sdk.python.rtdip_sdk.queries.time_series._query_builder import _query_builder


def get(connection: object, parameters_dict: dict) -> pd.DataFrame:
    '''
    A function that receives a dataframe of raw tag data and performs a timeweighted average, returning the results. 
    
    This function requires the input of a pandas dataframe acquired via the rtdip.functions.raw() method and the user to input a dictionary of parameters. (See Attributes table below)
    
    Pi data points will either have step enabled (True) or step disabled (False). You can specify whether you want step to be fetched by "Pi" or you can set the step parameter to True/False in the dictionary below.
    
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
        window_size_mins (int): Window size in minutes
        window_length (int): (Optional) add longer window time in days for the start or end of specified date to cater for edge cases
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        step (str): data points with step "enabled" or "disabled". The options for step are "metadata", "true" or "false". "metadata" will get the step requirements from the metadata table if applicable.
    Returns:
        DataFrame: A dataframe containing the time weighted averages.
    '''
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")

    try:
        query = _query_builder(parameters_dict, "time_weighted_average")

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
        logging.exception('error with time weighted average function')
        raise e

from src.sdk.python.rtdip_sdk.authentication.azure import DefaultAuth
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
#testing 
auth = DefaultAuth(exclude_visual_studio_code_credential=True).authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("adb-8969364155430721.1.azuredatabricks.net", "/sql/1.0/endpoints/9ecb6a8d6707260c", token)

parameters = {
    "business_unit": "downstream",
    "region": "EMEA",
    "asset": "pernis",
    "data_security_level": "restricted",
    "data_type": "float",
    "tag_names": ["SRU:050QR012.PV"],
    "start_date": "2023-01-01T23:00:00+0000",
    "end_date": "2023-01-02T06:00:00+0000",
    "window_size_mins": 15, 
    "step" : "false",
    "window_length": 1, 
    "include_bad_data": False, #options: [True, False]
}
x = get(connection, parameters)
print(x)
