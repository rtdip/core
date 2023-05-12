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
    """
    Units of measurement
    """
    w = 0 
    """Watts"""
    wh = 1
    """Watts/Hour"""
    kw = 2
    """Kilowatts"""
    kwh = 3
    """Kilowatts/Hour"""
    mw = 4
    """Megawatts"""
    mwh = 5
    """Megawatts/Hour"""


class SeriesType(IntFlag):
    """
    Definition of the type of timeseries for the measurements (e.g. realtime or interval based) and the type of the computation if the series is aggregated/derived 
    """
    real_time = auto()
    """
    The data has no specific time pattern
    """
    minute_1 = auto()
    """
    1 minute interval
    """
    minutes_5 = auto()
    """
    5 minutes interval 
    """
    minutes_10 = auto()
    """
    10 minutes interval 
    """
    minutes_15 = auto()
    """
    15 minutes interval 
    """
    minutes_30 = auto()
    """
    30 minutes interval
    """
    hour = auto()
    """
    60 minutes/1 hour interval
    """
    hours_2 = auto()
    """
    2 hours interval
    """
    hours_3 = auto()
    """
    3 hours interval
    """
    hours_4 = auto()
    """
    4 hours interval
    """
    hours_5 = auto()
    """
    5 hours interval
    """
    hours_6 = auto()
    """
    6 hours interval
    """
    hours_8 = auto()
    """
    8 hours interval
    """
    hours_12 = auto()
    """
    12 hours interval
    """
    hours_24 = auto()
    """
    1 Minute interval
    """
    week = auto()
    """
    1 Minute interval
    """
    month = auto()
    """
    1 Minute interval
    """
    year = auto()
    """
    1 Minute interval
    """
    sum = auto()
    """
    Measurement is the result of computing the sum of a set of measurements
    """
    mean_filter = auto()
    """
    Measurement is the result of computing the mean of a set of measurements
    """
    median_filter = auto()
    """
    Measurement is the result of computing the median of a set of measurements
    """
    max_filter = auto()
    """
    Measurement is the result of computing the max of a set of measurements
    """
    min_filter = auto()
    # Testing
    test = auto()



class Usage(BaseModel):
    """
    Usage. a usage measurement from an AMI meter 
    """
    uid: str  
    """
    A unique identifier associated to the source of the measurement (e.g. sensor, meter, etc.)
    """
    series_id: str
    """
    Identifier for a particular timeseries set
    """
    timestamp: int
    """
    Creation time. Always UTC. Seconds since EPOCH
    """
    interval_timestamp: int
    """
    The timestamp for the interval. Always UTC. Seconds since EPOCH
    """
    value: float
    """
    The actual value of the measurement
    """



class ValueType(IntFlag):
    """
    Defines the type of value
    """
    counter = auto()
    """
    The value is cumulative increasing monotinically
    """   
    gauge = auto()
    """
    The value can arbitrarily go up and down
    """
    histogram = auto()
    """
    The value is a histogram
    """
    summary = auto()
    """
    """
    usage = auto()
    """
    The value is from source that consumes energy
    """
    generation = auto()
    """
    The value is from a source that generates energy
    """
    prediction = auto()
    """
    The value is generated from a predictive model
    """
    short_term = auto()
    """
    The value is related to a short term (e.g. short term forecast)
    """
    long_term = auto()
    """
    The value is related to a long term (e.g. long term forecast)
    """
    actuals = auto()
    """
    The value is from a actual measurement
    """
    backcast = auto()
    """
    The value is related to a forecast that happens in the past (e.g. for calculating how good was the forecast compared to actuals)
    """
    forecast = auto()
    """
    The value is related to a forecast in the future
    """
    short_term_backcast = short_term | backcast
    long_term_term_backcast = long_term | backcast
    short_term_forecast = short_term | forecast
    long_term_term_forecast = long_term | forecast

class MetaData(BaseModel):
    """
    Metadata for a sensor, meter, etc. and its association to sets of time series
    """
    uid: str
    """
    A unique identifier associated to the source of the measurement (e.g. sensor, meter, etc.)
    """
    series_id: str
    """
    Identifier for a particular timeseries set
    """
    series_parent_id: str
    """
    Hierarchy (Sequence) of this TS associated to the same group of TS
    """
    name: str
    """
    Name of the sensor
    """
    uom: UomUsage 
    """
    Unit of measure for this sensor
    """
    description: str
    """
    Short description
    """
    timestamp_start: int
    """
    Timestamp of the creation of the record and start of the timeseries.  Always UTC. Seconds since EPOCH
    """
    timestamp_end: int 
    """
    Timestamp of end of the timeseries.  Always UTC. Seconds since EPOCH
    """
    time_zone: str
    """
    Time zone of where the sensor or where the series has started
    """
    version: str 
    """
    For versioning
    """
    series_type: SeriesType 
    """
    Type of the timeseries
    """
    model_type: ModelType
    """
    Type of model use to produce this data (e.g. a precitive model)
    """
    value_type: ValueType
    """
    Type of value of the timeseries
    """
    properties: dict  
    """
    Any other additional properties (Key/Value)
    """