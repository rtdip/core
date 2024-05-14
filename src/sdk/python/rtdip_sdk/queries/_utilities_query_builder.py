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

from jinja2 import Template
import datetime
from datetime import datetime, time

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def _is_date_format(dt, format):
    try:
        return datetime.strptime(dt, format)
    except Exception:
        return False


def _parse_date(dt, is_end_date=False, exclude_date_format=False):  # NOSONAR
    if isinstance(dt, datetime):
        if dt.time() == time.min and not is_end_date:
            if dt.tzinfo is not None:
                dt = datetime.strftime(dt, "%Y-%m-%d%z")
            else:
                dt = dt.date()
        else:
            dt = datetime.strftime(dt, TIMESTAMP_FORMAT)
    dt = str(dt)

    if _is_date_format(dt, "%Y-%m-%d") and exclude_date_format == False:
        _time = "T23:59:59" if is_end_date == True else "T00:00:00"
        return dt + _time + "+00:00"
    elif _is_date_format(dt, "%Y-%m-%dT%H:%M:%S"):
        return dt + "+00:00"
    elif _is_date_format(dt, TIMESTAMP_FORMAT):
        return dt
    elif _is_date_format(dt, "%Y-%m-%d%z"):
        _time = "T23:59:59" if is_end_date == True else "T00:00:00"
        dt = dt[0:10] + _time + dt[10:]
        return dt
    else:
        msg = f"Inputted timestamp: '{dt}', is not in the correct format."
        if exclude_date_format == True:
            msg += " List of timestamps must be in datetime format."
        raise ValueError(msg)


def _parse_dates(parameters_dict):  # NOSONAR
    if "start_date" in parameters_dict:
        parameters_dict["start_date"] = _parse_date(parameters_dict["start_date"])
        sample_dt = parameters_dict["start_date"]
    if "end_date" in parameters_dict:
        parameters_dict["end_date"] = _parse_date(parameters_dict["end_date"], True)
    if "timestamps" in parameters_dict:
        parsed_timestamp = [
            _parse_date(dt, is_end_date=False, exclude_date_format=True)
            for dt in parameters_dict["timestamps"]
        ]
        parameters_dict["timestamps"] = parsed_timestamp
        sample_dt = parsed_timestamp[0]

    parameters_dict["time_zone"] = datetime.strptime(
        sample_dt, TIMESTAMP_FORMAT
    ).strftime("%z")

    return parameters_dict


def _convert_to_seconds(s):
    return int(s[:-1]) * seconds_per_unit[s[-1]]
