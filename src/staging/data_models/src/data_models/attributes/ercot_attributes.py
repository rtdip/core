from dataclasses_json import dataclass_json
from dataclasses import dataclass
import datetime


@dataclass_json
@dataclass(frozen=True)
class ErcotAttributes:
    uid_str: str
    market: str
    flow_start: datetime.date
    flow_end: datetime.date
    zone: str
    utility: str
    post_code: str
    loss_code: str
    load_code_profile: str
