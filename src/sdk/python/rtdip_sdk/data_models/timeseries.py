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
from pydantic.v1 import BaseModel
from enum import Enum


class Uom(Enum):
    """
    Units of measurement
    """

    W = 0
    """Watts"""
    WH = 1
    """Watts/Hour"""
    KW = 2
    """Kilowatts"""
    KWH = 3
    """Kilowatts/Hour"""
    MW = 4
    """Megawatts"""
    MWH = 5
    """Megawatts/Hour"""
    WEATHER = 6
    """Weather related"""


class ModelType(IntFlag):
    """
    Specifies the type of model that will be represneted
    """

    Default = auto()
    """Default value"""
    AMI_USAGE = auto()
    """Advanced Meter Infrastructure usage"""
    WEATHER_AG2 = auto()
    """Atmospheric G2 Format"""
    WEATHER_ECMWF = auto()
    """European Centre for Medium-Range Weather Forecasts Format"""


class SeriesType(IntFlag):
    """
    Definition of the type of timeseries for the measurements (e.g. realtime or interval based) and the type of the computation if the series is aggregated/derived
    """

    RealTime = auto()
    """
    The data has no specific time pattern
    """
    Minute1 = auto()
    """
    1 minute interval
    """
    Minutes5 = auto()
    """
    5 minutes interval 
    """
    Minutes10 = auto()
    """
    10 minutes interval 
    """
    Minutes15 = auto()
    """
    15 minutes interval 
    """
    Minutes30 = auto()
    """
    30 minutes interval
    """
    Hour = auto()
    """
    60 minutes/1 hour interval
    """
    Hours2 = auto()
    """
    2 hours interval
    """
    Hours3 = auto()
    """
    3 hours interval
    """
    Hours4 = auto()
    """
    4 hours interval
    """
    Hours5 = auto()
    """
    5 hours interval
    """
    Hours6 = auto()
    """
    6 hours interval
    """
    Hours8 = auto()
    """
    8 hours interval
    """
    Hours12 = auto()
    """
    12 hours interval
    """
    Hours24 = auto()
    """
    1 Minute interval
    """
    Week = auto()
    """
    1 Minute interval
    """
    Month = auto()
    """
    1 Minute interval
    """
    Year = auto()
    """
    1 Minute interval
    """
    Sum = auto()
    """
    Measurement is the result of computing the sum of a set of measurements
    """
    MeanFilter = auto()
    """
    Measurement is the result of computing the mean of a set of measurements
    """
    MedianFilter = auto()
    """
    Measurement is the result of computing the median of a set of measurements
    """
    MaxFilter = auto()
    """
    Measurement is the result of computing the max of a set of measurements
    """
    MinFilter = auto()
    # Testing
    Test = auto()


class ValueType(IntFlag):
    """
    Defines the type of value
    """

    Counter = auto()
    """
    The value is cumulative increasing monotinically
    """
    Gauge = auto()
    """
    The value can arbitrarily go up and down
    """
    Histogram = auto()
    """
    The value is a histogram
    """
    Summary = auto()
    """
    """
    Usage = auto()
    """
    The value is from source that consumes energy
    """
    Generation = auto()
    """
    The value is from a source that generates energy
    """
    Prediction = auto()
    """
    The value is generated from a predictive model
    """
    ShortTerm = auto()
    """
    The value is related to a short term (e.g. short term forecast)
    """
    LongTerm = auto()
    """
    The value is related to a long term (e.g. long term forecast)
    """
    Actuals = auto()
    """
    The value is from a actual measurement
    """
    Backcast = auto()
    """
    The value is related to a forecast that happens in the past (e.g. for calculating how good was the forecast compared to actuals)
    """
    Forecast = auto()
    """
    The value is related to a forecast in the future
    """
    ShortTermBackcast = ShortTerm | Backcast
    LongTermBackcast = LongTerm | Backcast
    ShortTermForecast = ShortTerm | Forecast
    LongTermForecast = LongTerm | Forecast


class MetaData(BaseModel):
    """
    Metadata for a sensor, meter, etc. and its association to sets of time series
    """

    Uid: str
    """
    A unique identifier associated to the source of the measurement (e.g. sensor, meter, etc.)
    """
    SeriesId: str
    """
    Identifier for a particular timeseries set
    """
    SeriesParentId: str
    """
    Hierarchy (Sequence) of this TS associated to the same group of TS
    """
    Name: str
    """
    Name of the sensor
    """
    Uom: Uom
    """
    Unit of measure for this sensor
    """
    Description: str
    """
    Short description
    """
    TimestampStart: int
    """
    Timestamp of the creation of the record and start of the timeseries.  Always UTC. Seconds since EPOCH
    """
    TimestampEnd: int
    """
    Timestamp of end of the timeseries.  Always UTC. Seconds since EPOCH
    """
    Timezone: str
    """
    Time zone of where the sensor or where the series has started
    """
    Version: str
    """
    For versioning
    """
    SeriesType: SeriesType
    """
    Type of the timeseries
    """
    ModelType: ModelType
    """
    Type of model use to produce this data (e.g. a precitive model)
    """
    ValueType: ValueType
    """
    Type of value of the timeseries
    """
    Properties: dict
    """
    Any other additional properties (Key/Value)
    """
