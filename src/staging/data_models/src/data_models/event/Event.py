from dataclasses_json import dataclass_json
from dataclasses import dataclass


@dataclass_json
@dataclass(frozen=True)
class Event:
    uid: str  # Unique identifier (e.g. sensor, meter, etc)
    series_id: str  # Identifier for a particular time series
    timestamp: int  # Seconds since EPOCH. UTC
    status: int  # Status of the event #TODO define status as a class?
    value_float: float  # Null if value_str is not null
    value_str: str  # Null if value_float is not null
