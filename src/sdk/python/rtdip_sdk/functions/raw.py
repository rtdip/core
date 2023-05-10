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
import pytz
import pandas as pd
from src.sdk.python.rtdip_sdk.functions._query_builder import _query_builder
from datetime import datetime
import datetime

def get(connection: object, parameters_dict: dict) -> pd.DataFrame:
    '''
    A function to return back raw data by querying databricks SQL Warehouse using a connection specified by the user. 
    
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
        data_type (str): Type of the data (float, integer, double, string)
        tag_names (list): List of tagname or tagnames ["tag_1", "tag_2"]
        start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS)
        end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS)
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False

    Returns:
        DataFrame: A dataframe of raw timeseries data.
    '''
    try:
        query = _query_builder(parameters_dict)

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
        logging.exception('error with raw function')
        raise e
    
# from src.sdk.python.rtdip_sdk.authentication.authenticate import DefaultAuth
# from src.sdk.python.rtdip_sdk.odbc.db_sql_connector import DatabricksSQLConnection
# from src.sdk.python.rtdip_sdk.functions import raw
# #testing 
# auth = auth = DefaultAuth(exclude_visual_studio_code_credential=True).authenticate()
# token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
# connection = DatabricksSQLConnection("adb-8969364155430721.1.azuredatabricks.net", "/sql/1.0/endpoints/9ecb6a8d6707260c", token)
# dict = {
#     "business_unit": "downstream",
#     "region": "emea", 
#     "asset": "pernis", 
#     "data_security_level": "restricted", 
#     "data_type": "float",
#     "tag_names": ["PGP:720FY003.PV"], 
#     "start_date": datetime.datetime.now(),
#     "end_date": datetime.datetime.now(),
#     "include_bad_data": True,
# }
# x = raw.get(connection, dict)
# print(x)
