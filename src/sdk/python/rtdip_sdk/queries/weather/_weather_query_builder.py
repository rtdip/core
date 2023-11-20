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
from .._utilities_query_builder import (
    _is_date_format,
    _parse_date,
    _parse_dates,
    _convert_to_seconds,
)

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def _build_parameters(
    parameters_dict: dict,
    area_type: str,
    table_type: str,
) -> dict:
    if area_type == "grid":
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
            "limit": parameters_dict.get("limit", None),
            "latitude_column": parameters_dict.get("latitude_column", "Latitude"),
            "longitude_column": parameters_dict.get("longitude_column", "Longitude"),
            "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        }
        if table_type == "raw":
            raw_parameters["timestamp_column"] = parameters_dict.get(
                "timestamp_column", "EventTime"
            )
            raw_parameters["include_status"] = False

    if area_type == "point":
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
            "limit": parameters_dict.get("limit", None),
            "latitude_column": parameters_dict.get("latitude_column", "Latitude"),
            "longitude_column": parameters_dict.get("longitude_column", "Longitude"),
            "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        }
        if table_type == "raw":
            raw_parameters["timestamp_column"] = parameters_dict.get(
                "timestamp_column", "EventTime"
            )
            raw_parameters["include_status"] = False

    return raw_parameters


def _raw_query_grid(parameters_dict: dict) -> str:
    raw_query_grid = (
        "SELECT * FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ forecast|lower }}`.`weather`.`{{ region|lower }}_weather_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        'WHERE `{{ timestamp_column }}` BETWEEN to_timestamp("{{ start_date }}") AND to_timestamp("{{ end_date }}")'
        "AND `{{ latitude_column }}` > '{{ min_lat}}' "
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

    raw_parameters_grid = _build_parameters(parameters_dict, "grid", "raw")

    sql_template = Template(raw_query_grid)
    return sql_template.render(raw_parameters_grid)


def _raw_query_point(parameters_dict: dict) -> str:
    raw_query_point = (
        "SELECT * FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ forecast|lower }}`.`weather`.`{{ region|lower }}_weather_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        'WHERE `{{ timestamp_column }}` BETWEEN to_timestamp("{{ start_date }}") AND to_timestamp("{{ end_date }}")'
        "AND `{{ latitude_column }}` > '{{lat}}' "
        "AND `{{ longitude_column }}` > '{{lon}}' "
        "{% if source is defined and source is not none %}"
        "AND SOURCE = '{{ source }}' "
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
    )

    raw_parameters_point = _build_parameters(parameters_dict, "point", "raw")

    sql_template = Template(raw_query_point)
    return sql_template.render(raw_parameters_point)


def _latest_query_grid(parameters_dict: dict) -> str:
    latest_query_grid = (
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

    latest_parameters_grid = _build_parameters(parameters_dict, "grid", "latest")

    sql_template = Template(latest_query_grid)
    return sql_template.render(latest_parameters_grid)


def _latest_query_point(parameters_dict: dict) -> str:
    latest_query_point = (
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

    latest_parameters_point = _build_parameters(parameters_dict, "point", "latest")

    sql_template = Template(latest_query_point)
    return sql_template.render(latest_parameters_point)


def _query_builder(parameters_dict: dict, query_type: str) -> str:
    if query_type == "latest_point":
        return _latest_query_point(parameters_dict)

    if query_type == "latest_grid":
        return _latest_query_grid(parameters_dict)

    if query_type == "raw_point":
        return _raw_query_point(parameters_dict)

    if query_type == "raw_grid":
        return _raw_query_grid(parameters_dict)
