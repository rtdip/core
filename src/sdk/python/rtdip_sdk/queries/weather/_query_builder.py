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


def _parse_date(dt, is_end_date=False, exclude_date_format=False):
    if isinstance(dt, datetime):
        if dt.time() == time.min:
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


def _parse_dates(parameters_dict):
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


def _raw_query_grid(parameters_dict: dict) -> str:
    raw_query = (
        "SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`{{ timestamp_column }}`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS `{{ timestamp_column }}`, `{{ tagname_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% endif %} `{{ value_column }}` FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ forecast|lower }}`.`weather`.`{{ region|lower }}_weather_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        'WHERE `{{ timestamp_column }}` BETWEEN to_timestamp("{{ start_date }}") AND to_timestamp("{{ end_date }}") '
        "AND `{{ latitude_column }}` > '{{ min_lat}}' "
        "AND `{{ latitude_column }}` < '{{ max_lat}}' "
        "AND `{{ longitude_column }}` > '{{ min_lon}}' "
        "AND`{{ longitude_column }}` < '{{ max_lon}}' "
        "{% if source is defined and source is not none %}"
        "AND SOURCE = '{{ source }}' "
        "{% endif %}"
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %}"
        "AND `{{ status_column }}` = 'Good'"
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
    )

    raw_parameters = {
        "forecast": parameters_dict.get("forecast", None),
        "region": parameters_dict.get("region"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "max_lat": parameters_dict["max_lat"],
        "max_lon": parameters_dict["max_lon"],
        "min_lat": parameters_dict["min_lat"],
        "min_lon": parameters_dict["min_lon"],
        "source": parameters_dict.get("source", None),
        "include_bad_data": parameters_dict["include_bad_data"],
        "limit": parameters_dict.get("limit", None),
        "latitude_column": parameters_dict.get("latitude_column", "Latitude"),
        "longitude_column": parameters_dict.get("longitude_column", "Longitude"),
        "time_zone": parameters_dict["time_zone"],
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "timestamp_column": parameters_dict.get("timestamp_column", "EventTime"),
        "include_status": False
        if "status_column" in parameters_dict
        and parameters_dict.get("status_column") is None
        else True,
        "status_column": "Status"
        if "status_column" in parameters_dict
        and parameters_dict.get("status_column") is None
        else parameters_dict.get("status_column", "Status"),
        "value_column": parameters_dict.get("value_column", "Value"),
    }

    sql_template = Template(raw_query)
    return sql_template.render(raw_parameters)


def _raw_query_point(parameters_dict: dict) -> str:
    raw_query = (
        "SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`{{ timestamp_column }}`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS `{{ timestamp_column }}`, `{{ tagname_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% endif %} `{{ value_column }}` FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ forecast|lower }}`.`weather`.`{{ region|lower }}_weather_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        'WHERE `{{ timestamp_column }}` BETWEEN to_timestamp("{{ start_date }}") AND to_timestamp("{{ end_date }}") '
        "AND `{{ latitude_column }}` == '{{lat}}' "
        "AND `{{ longitude_column }}` == '{{lon}}' "
        "{% if source is defined and source is not none %}"
        "AND SOURCE = '{{ source }}' "
        "{% endif %}"
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %}"
        "AND `{{ status_column }}` = 'Good'"
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
    )

    raw_parameters = {
        "forecast": parameters_dict.get("forecast", None),
        "region": parameters_dict.get("region"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "lat": parameters_dict["lat"],
        "lon": parameters_dict["lon"],
        "source": parameters_dict.get("source", None),
        "include_bad_data": parameters_dict["include_bad_data"],
        "limit": parameters_dict.get("limit", None),
        "latitude_column": parameters_dict.get("latitude_column", "Latitude"),
        "longitude_column": parameters_dict.get("longitude_column", "Longitude"),
        "time_zone": parameters_dict["time_zone"],
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "timestamp_column": parameters_dict.get("timestamp_column", "EventTime"),
        "include_status": False
        if "status_column" in parameters_dict
        and parameters_dict.get("status_column") is None
        else True,
        "status_column": "Status"
        if "status_column" in parameters_dict
        and parameters_dict.get("status_column") is None
        else parameters_dict.get("status_column", "Status"),
        "value_column": parameters_dict.get("value_column", "Value"),
    }

    sql_template = Template(raw_query)
    return sql_template.render(raw_parameters)


def _latest_query_grid(parameters_dict: dict) -> str:
    latest_query = (
        "SELECT * FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ forecast|lower }}`.`weather`.`{{ region|lower }}_weather_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE `{{ latitude_column }}` > '{{ min_lat}}' "
        "AND `{{ latitude_column }}` < '{{ max_lat}}' "
        "AND `{{ longitude_column }}` > '{{ min_lon}}' "
        "AND`{{ longitude_column }}` < '{{ max_lon}}' "
        "{% if source is defined and source is not none %}"
        "AND SOURCE = '{{ source }}' "
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
    )

    latest_parameters = {
        "forecast": parameters_dict.get("forecast", None),
        "region": parameters_dict.get("region"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "max_lat": parameters_dict["max_lat"],
        "max_lon": parameters_dict["max_lon"],
        "min_lat": parameters_dict["min_lat"],
        "min_lon": parameters_dict["min_lon"],
        "source": parameters_dict.get("source", None),
        "limit": parameters_dict.get("limit", None),
        "latitude_column": parameters_dict.get("latitude_column", "Latitude"),
        "longitude_column": parameters_dict.get("longitude_column", "Longitude"),
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
    }

    sql_template = Template(latest_query)
    return sql_template.render(latest_parameters)


def _latest_query_point(parameters_dict: dict) -> str:
    latest_query = (
        "SELECT * FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ forecast|lower }}`.`weather`.`{{ region|lower }}_weather_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE `{{ latitude_column }}` == '{{lat}}' "
        "AND `{{ longitude_column }}` == '{{lon}}' "
        "{% if source is defined and source is not none %}"
        "AND SOURCE = '{{ source }}' "
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
    )

    latest_parameters = {
        "forecast": parameters_dict.get("forecast", None),
        "region": parameters_dict.get("region"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "lat": parameters_dict["lat"],
        "lon": parameters_dict["lon"],
        "source": parameters_dict.get("source", None),
        "limit": parameters_dict.get("limit", None),
        "latitude_column": parameters_dict.get("latitude_column", "Latitude"),
        "longitude_column": parameters_dict.get("longitude_column", "Longitude"),
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
    }

    sql_template = Template(latest_query)
    return sql_template.render(latest_parameters)


def _query_builder(parameters_dict: dict, query_type: str) -> str:
    if "tag_names" not in parameters_dict:
        parameters_dict["tag_names"] = []
    tagnames_deduplicated = list(
        dict.fromkeys(parameters_dict["tag_names"])
    )  # remove potential duplicates in tags
    parameters_dict["tag_names"] = tagnames_deduplicated.copy()

    parameters_dict = _parse_dates(parameters_dict)

    if query_type == "latest_point":
        return _latest_query_point(parameters_dict)

    if query_type == "latest_grid":
        return _latest_query_grid(parameters_dict)

    if query_type == "raw_point":
        return _raw_query_point(parameters_dict)

    if query_type == "raw_grif":
        return _raw_query_grid(parameters_dict)
