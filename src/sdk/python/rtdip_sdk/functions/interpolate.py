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
    An RTDIP interpolation function that is intertwined with the RTDIP Resampling function.
    
    The Interpolation function will forward fill or backward fill the resampled data depending users specified interpolation method.

    This function requires the user to input a dictionary of parameters. (See Attributes table below.)

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        parameters_dict: A dictionary of parameters (see Attributes table below)

    Attributes:
        buisness_unit (str): Business unit of the data
        region (str): Region
        asset (str):  Asset
        data_security_level (str): Level of data security 
        data_type (str): Type of the data (float, integer, double, string)
        tag_names (list): List of tagname or tagnames ["tag_1", "tag_2"]
        start_date (str): Start date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS)
        end_date (str): End date (Either a date in the format YY-MM-DD or a datetime in the format YYY-MM-DDTHH:MM:SS)
        sample_rate (int): The resampling rate (numeric input)
        sample_unit (str): The resampling unit (second, minute, day, hour)
        agg_method (str): Aggregation Method (first, last, avg, min, max)
        interpolation_method (str): Optional. Interpolation method (forward_fill, backward_fill)
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False

    Returns:
        DataFrame: A resampled and interpolated dataframe.
    '''
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")

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
        logging.exception('error with interpolate function')
        raise e