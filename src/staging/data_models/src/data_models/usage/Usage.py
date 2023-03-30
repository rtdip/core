from dataclasses_json import dataclass_json
from dataclasses import dataclass
import datetime


@dataclass_json
@dataclass(frozen=True)
class Usage(object):
    uid: str  # A unique identifier tied to a timeseries of data.
    series_id: str  # Identifier for a particular timeseries set
    timestamp: int  # Creation time. Always UTC. Seconds since EPOCH
    interval_timestamp: int  # The timestamp for the interval. Always UTC. Seconds since EPOCH.
    value: float  # The actual value of the measurement
