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

import re


def parse_time_string_to_ms(time_str: str) -> float:
    """
    Parses a time string and returns the total time in milliseconds.

    Args:
        time_str (str): Time string (e.g., '10ms', '1s', '2m', '1h').

    Returns:
        float: Total time in milliseconds.

    Raises:
        ValueError: If the format is invalid.
    """
    pattern = re.compile(r"^(\d+\.?\d*)(ms|s|m|h)$")
    match = pattern.match(time_str)
    if not match:
        raise ValueError(f"Invalid time format: {time_str}")
    value, unit = match.groups()
    value = float(value)
    if unit == "ms":
        return value
    elif unit == "s":
        return value * 1000
    elif unit == "m":
        return value * 60 * 1000
    elif unit == "h":
        return value * 3600 * 1000
    else:
        raise ValueError(f"Unsupported time unit in time: {unit}")
