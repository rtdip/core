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


from enum import IntFlag, auto
from pydantic import BaseModel
from enum import Enum


class ModelType(IntFlag):
    default = auto()


class UomUsage(Enum):
    w = 0
    wh = 1
    kw = 2
    kwh = 3
    mw = 4
    mwh = 5
    # Other


class SeriesType(IntFlag):
    # Keep the order when refactoring.
    real_time = auto()
    minute_1 = auto()
    minutes_5 = auto()
    minutes_10 = auto()
    minutes_15 = auto()
    minutes_30 = auto()
    minutes_60 = auto()
    minutes_2_hours = auto()
    minutes_3_hours = auto()
    minutes_4_hours = auto()
    minutes_6_hours = auto()
    minutes_8_hours = auto()
    minutes_12_hours = auto()
    minutes_24_hours = auto()
    hour = auto()
    day = auto()
    week = auto()
    month = auto()
    year = auto()
    # Computations
    sum = auto()
    average_filter = auto()
    max_filter = auto()
    min_filter = auto()
    # Testing
    test = auto()




class Usage(BaseModel):
    uid: str  # A unique identifier tied to a timeseries of data.
    series_id: str  # Identifier for a particular timeseries set
    timestamp: int  # Creation time. Always UTC. Seconds since EPOCH
    interval_timestamp: int  # The timestamp for the interval. Always UTC. Seconds since EPOCH.
    value: float  # The actual value of the measurement



class ValueType(IntFlag):
    # Keep the order when refactoring.
    counter = auto()
    gauge = auto()
    histogram = auto()
    summary = auto()
    usage = auto()
    generation = auto()
    prediction = auto()
    short_term = auto()
    long_term = auto()
    backcast = auto()
    forecast = auto()
    short_term_backcast = short_term | backcast
    long_term_term_backcast = long_term | backcast
    short_term_forecast = short_term | forecast
    long_term_term_forecast = long_term | forecast

class MetaData(BaseModel):
    uid: str  # Unique identifier (e.g. sensor, meter, etc)
    series_id: str  # Unique Identifier for a particular time series (TS)
    series_parent_id: str  # Hierarchy (Sequence) of this TS associated to the same group of TS
    name: str  # Name of the sensor
    uom: UomUsage  # Unit of measure for this sensor
    description: str  # Short description
    timestamp_start: int  # Timestamp of the creation of the record and start of the timeseries. UTC
    timestamp_end: int  # Timestamp of end of the timeseries. UTC
    time_zone: str  # Time zone of where the sensor or where the series has started
    version: str  # Version of this uid/series_id
    series_type: SeriesType  # Type of time series (e.g. pure time series, interval type, etc.)
    model_type: ModelType  # Type for this series (sensor type, measurement type, forecast type, etc)
    value_type: ValueType
    # for this time_series id
    properties: dict  # Any Additional properties (Key/Value)