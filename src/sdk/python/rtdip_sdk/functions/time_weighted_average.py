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
from .raw import get as raw_get
from .metadata import get as metadata_get
from datetime import datetime, timedelta
import pytz
import numpy as np

def get(connection: object, parameters_dict: dict) -> pd.DataFrame:
    '''
    A function that recieves a dataframe of raw tag data and performs a timeweighted average, returning the results. 
    
    This function requires the input of a pandas dataframe acquired via the rtdip.functions.raw() method and the user to input a dictionary of parameters. (See Attributes table below)
    
    Pi data points will either have step enabled (True) or step disabled (False). You can specify whether you want step to be fetched by "Pi" or you can set the step parameter to True/False in the dictionary below.
    
    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        parameter_dict (dict): A dictionary of parameters (see Attributes table below)

    Attributes:
        buisness_unit (str): Business unit 
        region (str): Region
        asset (str): Asset 
        data_security_level (str): Level of data security 
        data_type (str): Type of the data (float, integer, double, string)
        tag_names (list): List of tagname or tagnames
        start_date (str): Start date (Either a utc date in the format YYYY-MM-DD or a utc datetime in the format YYYY-MM-DDTHH:MM:SS)
        end_date (str): End date (Either a utc date in the format YYYY-MM-DD or a utc datetime in the format YYYY-MM-DDTHH:MM:SS)
        window_size_mins (int): Window size in minutes
        window_length (int): (Optional) add longer window time for the start or end of specified date to cater for edge cases
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        step (str/bool): data points with step "enabled" or "disabled". The options for step are "metadata" (string), True or False (bool)

    Returns:
        DataFrame: A dataframe containing the time weighted averages.
    '''
    try:
        datetime_format = "%Y-%m-%dT%H:%M:%S"
        utc="Etc/UTC"

        if len(parameters_dict["start_date"]) == 10:
            original_start_date = datetime.strptime(parameters_dict["start_date"] + "T00:00:00", datetime_format)
            parameters_dict["start_date"] = parameters_dict["start_date"] + "T00:00:00"
        else:
            original_start_date = datetime.strptime(parameters_dict["start_date"], datetime_format)

        if len(parameters_dict["end_date"]) == 10:
            original_end_date = datetime.strptime(parameters_dict["end_date"] + "T23:59:59", datetime_format) 
            parameters_dict["end_date"] = parameters_dict["end_date"] + "T23:59:59"
        else: 
            original_end_date = datetime.strptime(parameters_dict["end_date"], datetime_format)

        if "window_length" in parameters_dict:       
            parameters_dict["start_date"] = (datetime.strptime(parameters_dict["start_date"], datetime_format) - timedelta(minutes = int(parameters_dict["window_length"]))).strftime(datetime_format)
            parameters_dict["end_date"] = (datetime.strptime(parameters_dict["end_date"], datetime_format) + timedelta(minutes = int(parameters_dict["window_length"]))).strftime(datetime_format) 
        else:
            parameters_dict["start_date"] = (datetime.strptime(parameters_dict["start_date"], datetime_format) - timedelta(minutes = int(parameters_dict["window_size_mins"]))).strftime(datetime_format)
            parameters_dict["end_date"] = (datetime.strptime(parameters_dict["end_date"], datetime_format) + timedelta(minutes = int(parameters_dict["window_size_mins"]))).strftime(datetime_format)

        pandas_df = raw_get(connection, parameters_dict)

        pandas_df["EventDate"] = pd.to_datetime(pandas_df["EventTime"]).dt.date  

        boundaries_df = pd.DataFrame(columns=["EventTime", "TagName"])

        for tag in parameters_dict["tag_names"]:
            start_date_new_row = pd.DataFrame([[pd.to_datetime(parameters_dict["start_date"]).replace(tzinfo=pytz.timezone(utc)), tag]], columns=["EventTime", "TagName"])
            end_date_new_row = pd.DataFrame([[pd.to_datetime(parameters_dict["end_date"]).replace(tzinfo=pytz.timezone(utc)), tag]], columns=["EventTime", "TagName"])
            boundaries_df = pd.concat([boundaries_df, start_date_new_row, end_date_new_row], ignore_index=True)
        boundaries_df.set_index(pd.DatetimeIndex(boundaries_df["EventTime"]), inplace=True)
        boundaries_df.drop(columns="EventTime", inplace=True)
        boundaries_df = boundaries_df.groupby(["TagName"]).resample("{}T".format(str(parameters_dict["window_size_mins"]))).ffill().drop(columns='TagName')

        #preprocess - add boundaries and time interpolate missing boundary values
        preprocess_df = pandas_df.copy()
        preprocess_df["EventTime"] = preprocess_df["EventTime"].round("S")
        preprocess_df.set_index(["EventTime", "TagName", "EventDate"], inplace=True)
        preprocess_df = preprocess_df.join(boundaries_df, how="outer", rsuffix="right")
        if isinstance(parameters_dict["step"], str) and parameters_dict["step"].lower() == "metadata":
            metadata_df = metadata_get(connection, parameters_dict)
            metadata_df.set_index("TagName", inplace=True)
            metadata_df = metadata_df.loc[:, "Step"]
            preprocess_df = preprocess_df.merge(metadata_df, left_index=True, right_index=True)
        elif parameters_dict["step"] == True:
            preprocess_df["Step"] =  True
        elif parameters_dict["step"] == False:
            preprocess_df["Step"] = False
        else:
            raise Exception('Unexpected step value', parameters_dict["step"])

        def process_time_weighted_averages_step(pandas_df):
            if pandas_df["Step"].any() == False:
                pandas_df = pandas_df.reset_index(level=["TagName", "EventDate"]).sort_index().interpolate(method='time')
                shift_raw_df = pandas_df.copy()
                shift_raw_df["CalcValue"] = (shift_raw_df.index.to_series().diff().dt.seconds/86400) * shift_raw_df.Value.rolling(2).sum()
                time_weighted_averages = shift_raw_df.resample("{}T".format(str(parameters_dict["window_size_mins"])), closed="right", label="right").CalcValue.sum() * 0.5 / parameters_dict["window_size_mins"] * 24 * 60
                return time_weighted_averages
            else:
                pandas_df = pandas_df.reset_index(level=["TagName", "EventDate"]).sort_index().interpolate(method='pad', limit_direction='forward')
                shift_raw_df = pandas_df.copy()
                shift_raw_df["CalcValue"] = (shift_raw_df.index.to_series().diff().dt.seconds/86400) * shift_raw_df.Value.shift(1)
                time_weighted_averages = shift_raw_df.resample("{}T".format(str(parameters_dict["window_size_mins"])), closed="right", label="right").CalcValue.sum() / parameters_dict["window_size_mins"] * 24 * 60
                return time_weighted_averages

        #calculate time weighted averages
        time_weighted_averages = preprocess_df.groupby(["TagName"]).apply(process_time_weighted_averages_step).reset_index()
        time_weighted_averages = time_weighted_averages.melt(id_vars="TagName", var_name="EventTime", value_name="Value").set_index("EventTime").sort_values(by=["TagName", "EventTime"])
        time_weighted_averages_datetime = time_weighted_averages.index.to_pydatetime()
        weighted_averages_timezones = np.array([z.replace(tzinfo=pytz.timezone(utc)) for z in time_weighted_averages_datetime])
        time_weighted_averages = time_weighted_averages[(original_start_date.replace(tzinfo=pytz.timezone(utc)) < weighted_averages_timezones) & (weighted_averages_timezones <= original_end_date.replace(tzinfo=pytz.timezone(utc)) + timedelta(seconds = 1))]
        pd.set_option('display.max_rows', None)
        print(time_weighted_averages)
        return time_weighted_averages
        
    except Exception as e:
        logging.exception('error with time weighted average function', str(e))
        raise e