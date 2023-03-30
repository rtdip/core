from dataclasses_json import dataclass_json
from dataclasses import dataclass
import datetime

# For partitioning and in serialized form, the implementation of (e.g. datetime.datetime type) must be URL friendly (e.g. no encoding)
# For example invalid characters might be (this is not an exaustive list) &,$,@,=

# Placeholder for now
@dataclass_json
@dataclass(frozen=True)
class Usage:
    uid: str  # A unique identifier tied to a time-series of data.
    # Examples include ESIID,
    # MPAN, MPRN. It can also be
    # a combination of attributes that makes it unique
    proxy_date: datetime.date  # YYYY-MM-DD
    time_zone: str  # https://pypi.org/project/pytz/
    version: str
    value_interval: str  # ENUM (int01, int02,...).
    # Local Interval of start_time
    value_type: str  # ENUM(NET, UFE, Distribution,
    # Transmission, Generation, Gross, Base, Time, Weather). Type of
    # Load represented in the data. (Loss Adjustments, load disaggregation levels)
    value: float  # The actual value
    uom: str  # ENUM (kWh, kW, MWh, MW, …). Unit of measurement
    model_type: str  # ENUM. Use “Actual” for default value.
    # Other values are Hybrid, RCE
    process_time: datetime.datetime  # Record processing time. Always UTC from processor.
    remote_process_time: datetime.datetime  # Time client processed the record.
    # It might be null for some clients.
    modify_time: datetime.date  # Record processing time.
    # Always UTC from processor.
