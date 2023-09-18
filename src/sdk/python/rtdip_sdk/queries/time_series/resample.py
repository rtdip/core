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
    """
    An RTDIP Resampling function in spark to resample data by querying databricks SQL warehouses using a connection and authentication method specified by the user. This spark resample function will return a resampled dataframe.

    The available connectors by RTDIP are Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect.

    The available authentication methods are Certificate Authentication, Client Secret Authentication or Default Authentication. See documentation.

    This function requires the user to input a dictionary of parameters. (See Attributes table below)

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
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
        sample_rate (int): (deprecated) Please use time_interval_rate instead. See below.
        sample_unit (str): (deprecated) Please use time_interval_unit instead. See below.
        time_interval_rate (str): The time interval rate (numeric input)
        time_interval_unit (str): The time interval unit (second, minute, day, hour)
        agg_method (str): Aggregation Method (first, last, avg, min, max)
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        pivot (bool): Pivot the data on timestamp column with True or do not pivot the data with False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows

    Returns:
        DataFrame: A resampled dataframe.
    """
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")

    if "sample_rate" in parameters_dict:
        logging.warning(
            "Parameter sample_rate is deprecated and will be removed in v1.0.0. Please use time_interval_rate instead."
        )
        parameters_dict["time_interval_rate"] = parameters_dict["sample_rate"]

    if "sample_unit" in parameters_dict:
        logging.warning(
            "Parameter sample_unit is deprecated and will be removed in v1.0.0. Please use time_interval_unit instead."
        )
        parameters_dict["time_interval_unit"] = parameters_dict["sample_unit"]

    try:
        query = _query_builder(parameters_dict, "resample")

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
        logging.exception("error with resampling function")
        raise e
