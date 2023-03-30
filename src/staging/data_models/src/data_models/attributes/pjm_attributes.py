from dataclasses_json import dataclass_json
from dataclasses import dataclass
import datetime


@dataclass_json
@dataclass(frozen=True)
class PjmAttributes:
    uid: str
    market: str
    flow_start: datetime.date
    flow_end: datetime.date
    utility: str
    meter_type: str
    post_code: str
    voltage_lvl: str
    load_code_profile: str
