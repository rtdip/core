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
    An RTDIP interpolation at time function which works out the linear interpolation at a specific time based on the points before and after.
    
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
        tag_names (str): Name of the tag
        timestamps (list): List of timestamp or timestamps in the format YYY-MM-DDTHH:MM:SS or YYY-MM-DDTHH:MM:SS+zz:zz where %z is the timezone. (Example +00:00 is the UTC timezone)
        window_length (int): Add longer window time in days for the start or end of specified date to cater for edge cases.
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False

    Returns:
        DataFrame: A interpolated at time dataframe.
    '''
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")

    if isinstance(parameters_dict["timestamps"], list) is False:
        raise ValueError("timestamps must be a list")

    try:
        query = _query_builder(parameters_dict, "interpolation_at_time")

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