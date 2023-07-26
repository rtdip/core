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
from ._query_builder import _query_builder

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
        window_size_mins (int): (deprecated) Window size in minutes. Please use time_interval_rate and time_interval_unit below instead.
        time_interval_rate (str): The time interval rate (numeric input)
        time_interval_unit (str): The time interval unit (second, minute, day, hour)
        window_length (int): Add longer window time in days for the start or end of specified date to cater for edge cases.
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        step (str): data points with step "enabled" or "disabled". The options for step are "metadata", "true" or "false". "metadata" will get the step requirements from the metadata table if applicable.
    Returns:
        DataFrame: A dataframe containing the time weighted averages.
    '''
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")
    
    if "window_size_mins" in parameters_dict:
        logging.warning('Parameter window_size_mins is deprecated and will be removed in v1.0.0. Please use time_interval_rate and time_interval_unit instead.')
        parameters_dict["time_interval_rate"] = str(parameters_dict["window_size_mins"])
        parameters_dict["time_interval_unit"] = "minute"

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