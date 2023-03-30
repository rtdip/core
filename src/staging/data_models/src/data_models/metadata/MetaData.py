from dataclasses_json import dataclass_json
from data_models.series import SeriesType
from data_models.usage import ValueType
from data_models.usage import ModelType
from dataclasses import dataclass

@dataclass_json
@dataclass(frozen=True)
class MetaData(object):
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
