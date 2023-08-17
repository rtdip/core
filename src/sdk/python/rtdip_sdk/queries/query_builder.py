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

from typing import Union
from ..connectors.connection_interface import ConnectionInterface
from .time_series import raw, resample, interpolate, interpolation_at_time, time_weighted_average, circular_average, circular_standard_deviation
from . import metadata

class QueryBuilder():
    parameters: dict
    connection: ConnectionInterface
    data_source: str
    tagname_column: str
    timestamp_column: str
    status_column: str
    value_column: str

    def connect(self, connection: ConnectionInterface):
        self.connection = connection
        return self

    def source(self, source: str, tagname_column: str = "TagName", timestamp_column: str = "EventTime", status_column: Union[str, None] = "Status", value_column: str = "Value"):
        self.data_source = "`.`".join(source.split("."))
        self.tagname_column = tagname_column
        self.timestamp_column = timestamp_column
        self.status_column = status_column
        self.value_column = value_column
        return self
    
    def raw(self, tagname_filter: [str], start_date: str, end_date: str, include_bad_data: bool = False):
        raw_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column            
        }
        return raw.get(self.connection, raw_parameters)
    
    def resample(self, tagname_filter: [str], start_date: str, end_date: str, time_interval_rate: str, time_interval_unit: str, agg_method: str, include_bad_data: bool = False):
        resample_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "agg_method": agg_method,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column            
        }

        return resample.get(self.connection, resample_parameters)
    
    def interpolate(self, tagname_filter: [str], start_date: str, end_date: str, time_interval_rate: str, time_interval_unit: str, agg_method: str, interpolation_method: str, include_bad_data: bool = False):
        interpolation_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "agg_method": agg_method,
            "interpolation_method": interpolation_method,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column           
        }

        return interpolate.get(self.connection, interpolation_parameters)
    
    def interpolation_at_time(self, tagname_filter: [str], timestamp_filter: list[str], include_bad_data: bool = False, window_length: int = 1):
        interpolation_at_time_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "timestamps": timestamp_filter,
            "include_bad_data": include_bad_data,
            "window_length": window_length,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column              
        }

        return interpolation_at_time.get(self.connection, interpolation_at_time_parameters)
    
    def time_weighted_average(self, tagname_filter: [str], start_date: str, end_date: str, time_interval_rate: str, time_interval_unit: str, step: str, source_metadata: str = None, include_bad_data: bool = False, window_length: int = 1):
        time_weighted_average_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "step": step,
            "source_metadata": "`.`".join(source_metadata.split(".")),
            "window_length": window_length,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column        
        }

        return time_weighted_average.get(self.connection, time_weighted_average_parameters)

    def metadata(self, tagname_filter: [str]):
        metadata_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "tagname_column": self.tagname_column,     
        }

        return metadata.get(self.connection, metadata_parameters)
    
    def circular_average(self, tagname_filter: [str], start_date: str, end_date: str, time_interval_rate: str, time_interval_unit: str, lower_bound: int, upper_bound: int, include_bad_data: bool = False):
        circular_average_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column           
        }

        return circular_average.get(self.connection, circular_average_parameters)
    
    def circular_standard_deviation(self, tagname_filter: [str], start_date: str, end_date: str, time_interval_rate: str, time_interval_unit: str, lower_bound: int, upper_bound: int, include_bad_data: bool = False):
        circular_stddev_parameters = {
            "source": self.data_source,
            "tag_names": tagname_filter,
            "start_date": start_date,
            "end_date": end_date,
            "include_bad_data": include_bad_data,
            "time_interval_rate": time_interval_rate,
            "time_interval_unit": time_interval_unit,
            "lower_bound": lower_bound,
            "upper_bound": upper_bound,
            "tagname_column": self.tagname_column,
            "timestamp_column": self.timestamp_column,
            "status_column": self.status_column,
            "value_column": self.value_column           
        }
             
        return circular_standard_deviation.get(self.connection, circular_stddev_parameters)    