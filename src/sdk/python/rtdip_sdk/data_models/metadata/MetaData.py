
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

from data_models.series import SeriesType
from data_models.usage import ValueType
from data_models.usage import ModelType
from dataclasses import dataclass
from pydantic import BaseModel

class MetaData(BaseModel):
    uid: str  # Unique identifier (e.g. sensor, meter, etc)
    series_id: str  # Unique Identifier for a particular time series (TS)
    series_parent_id: str  # Hierarchy (Sequence) of this TS associated to the same group of TS
    name: str  # Name of the sensor
    uom: int  # Unit of measure for this sensor
    description: str  # Short description
    timestamp_start: int  # Timestamp of the creation of the record and start of the timeseries. UTC
    timestamp_end: int  # Timestamp of end of the timeseries. UTC
    time_zone: str  # Time zone of where the sensor or where the series has started
    version: str  # Version of this uid/series_id
    series_type: SeriesType.SeriesType  # Type of time series (e.g. pure time series, interval type, etc.)
    model_type: ModelType.ModelType  # Type for this series (sensor type, measurement type, forecast type, etc)
    value_type: ValueType.ValueType
    # for this time_series id
    properties: dict  # Any Additional properties (Key/Value)
