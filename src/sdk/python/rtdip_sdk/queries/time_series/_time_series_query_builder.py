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
import logging
from datetime import datetime, time
from .._utilities_query_builder import (
    _is_date_format,
    _parse_date,
    _parse_dates,
    _convert_to_seconds,
)

TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
seconds_per_unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}


def _raw_query(parameters_dict: dict) -> str:
    raw_query = (
        "SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`{{ timestamp_column }}`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS `{{ timestamp_column }}`, `{{ tagname_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% endif %} `{{ value_column }}` FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE `{{ timestamp_column }}` BETWEEN to_timestamp(\"{{ start_date }}\") AND to_timestamp(\"{{ end_date }}\") AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %}"
        "AND `{{ status_column }}` = 'Good'"
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    raw_parameters = {
        "source": parameters_dict.get("source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "include_bad_data": parameters_dict["include_bad_data"],
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "time_zone": parameters_dict["time_zone"],
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "timestamp_column": parameters_dict.get("timestamp_column", "EventTime"),
        "include_status": (
            False
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else True
        ),
        "status_column": (
            "Status"
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else parameters_dict.get("status_column", "Status")
        ),
        "value_column": parameters_dict.get("value_column", "Value"),
    }

    sql_template = Template(raw_query)
    return sql_template.render(raw_parameters)


def _sample_query(parameters_dict: dict) -> tuple:
    sample_query = (
        "WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`{{ timestamp_column }}`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS `{{ timestamp_column }}`, `{{ tagname_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% else %} 'Good' AS `Status`, {% endif %} `{{ value_column }}` FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE `{{ timestamp_column }}` BETWEEN to_timestamp(\"{{ start_date }}\") AND to_timestamp(\"{{ end_date }}\") AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND `{{ status_column }}` = 'Good' {% endif %}) "
        ',date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp("{{ start_date }}"), "{{ time_zone }}"), from_utc_timestamp(to_timestamp("{{ end_date }}"), "{{ time_zone }}"), INTERVAL \'{{ time_interval_rate + \' \' + time_interval_unit }}\')) AS timestamp_array) '
        ",window_buckets AS (SELECT timestamp_array AS window_start, timestampadd({{time_interval_unit }}, {{ time_interval_rate }}, timestamp_array) AS window_end FROM date_array) "
        ",resample AS (SELECT /*+ RANGE_JOIN(d, {{ range_join_seconds }} ) */ d.window_start, d.window_end, e.`{{ tagname_column }}`, {{ agg_method }}(e.`{{ value_column }}`) OVER (PARTITION BY e.`{{ tagname_column }}`, d.window_start ORDER BY e.`{{ timestamp_column }}` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `{{ value_column }}` FROM window_buckets d INNER JOIN raw_events e ON d.window_start <= e.`{{ timestamp_column }}` AND d.window_end > e.`{{ timestamp_column }}`) "
        ",project AS (SELECT window_start AS `{{ timestamp_column }}`, `{{ tagname_column }}`, `{{ value_column }}` FROM resample GROUP BY window_start, `{{ tagname_column }}`, `{{ value_column }}` "
        "{% if is_resample is defined and is_resample == true %}"
        "ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% endif %}"
        ") "
        "{% if is_resample is defined and is_resample == true and pivot is defined and pivot == true %}"
        ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
        "{% for i in range(tag_names | length) %}"
        "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
        "{% endfor %}"
        "))) SELECT * FROM pivot ORDER BY `{{ timestamp_column }}` "
        "{% else %}"
        "SELECT * FROM project "
        "{% endif %}"
        "{% if is_resample is defined and is_resample == true and limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if is_resample is defined and is_resample == true and offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    sample_parameters = {
        "source": parameters_dict.get("source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "include_bad_data": parameters_dict["include_bad_data"],
        "time_interval_rate": parameters_dict["time_interval_rate"],
        "time_interval_unit": parameters_dict["time_interval_unit"],
        "agg_method": parameters_dict["agg_method"],
        "time_zone": parameters_dict["time_zone"],
        "pivot": parameters_dict.get("pivot", None),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "is_resample": True,
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "timestamp_column": parameters_dict.get("timestamp_column", "EventTime"),
        "include_status": (
            False
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else True
        ),
        "status_column": (
            "Status"
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else parameters_dict.get("status_column", "Status")
        ),
        "value_column": parameters_dict.get("value_column", "Value"),
        "range_join_seconds": parameters_dict["range_join_seconds"],
    }

    sql_template = Template(sample_query)
    sql_query = sql_template.render(sample_parameters)
    return sql_query, sample_query, sample_parameters


def _interpolation_query(
    parameters_dict: dict, sample_query: str, sample_parameters: dict
) -> str:
    if parameters_dict["interpolation_method"] == "forward_fill":
        interpolation_methods = "last_value/UNBOUNDED PRECEDING/CURRENT ROW"

    if parameters_dict["interpolation_method"] == "backward_fill":
        interpolation_methods = "first_value/CURRENT ROW/UNBOUNDED FOLLOWING"

    if (
        parameters_dict["interpolation_method"] == "forward_fill"
        or parameters_dict["interpolation_method"] == "backward_fill"
    ):
        interpolation_options = interpolation_methods.split("/")

    interpolate_query = (
        f"WITH resample AS ({sample_query})"
        ',date_array AS (SELECT DISTINCT explode(sequence(from_utc_timestamp(to_timestamp("{{ start_date }}"), "{{ time_zone }}"), from_utc_timestamp(to_timestamp("{{ end_date }}"), "{{ time_zone }}"), INTERVAL \'{{ time_interval_rate + \' \' + time_interval_unit }}\')) AS `{{ timestamp_column }}`, explode(array(`{{ tagname_column }}`)) AS `{{ tagname_column }}` FROM resample) '
        '{% if (interpolation_method is defined) and (interpolation_method == "forward_fill" or interpolation_method == "backward_fill") %}'
        ",project AS (SELECT a.`{{ timestamp_column }}`, a.`{{ tagname_column }}`, {{ interpolation_options_0 }}(b.`{{ value_column }}`, true) OVER (PARTITION BY a.`{{ tagname_column }}` ORDER BY a.`{{ timestamp_column }}` ROWS BETWEEN {{ interpolation_options_1 }} AND {{ interpolation_options_2 }}) AS `{{ value_column }}` FROM date_array a LEFT OUTER JOIN resample b ON a.`{{ timestamp_column }}` = b.`{{ timestamp_column }}` AND a.`{{ tagname_column }}` = b.`{{ tagname_column }}`) "
        '{% elif (interpolation_method is defined) and (interpolation_method == "linear") %}'
        ",linear_interpolation_calculations AS (SELECT coalesce(a.`{{ tagname_column }}`, b.`{{ tagname_column }}`) AS `{{ tagname_column }}`, coalesce(a.`{{ timestamp_column }}`, b.`{{ timestamp_column }}`) AS `{{ timestamp_column }}`, a.`{{ timestamp_column }}` AS `Requested_{{ timestamp_column }}`, b.`{{ timestamp_column }}` AS `Found_{{ timestamp_column }}`, b.`{{ value_column }}`, "
        "last_value(b.`{{ timestamp_column }}`, true) OVER (PARTITION BY a.`{{ tagname_column }}` ORDER BY a.`{{ timestamp_column }}` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Last_{{ timestamp_column }}`, last_value(b.`{{ value_column }}`, true) OVER (PARTITION BY a.`{{ tagname_column }}` ORDER BY a.`{{ timestamp_column }}` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Last_{{ value_column }}`, "
        "first_value(b.`{{ timestamp_column }}`, true) OVER (PARTITION BY a.`{{ tagname_column }}` ORDER BY a.`{{ timestamp_column }}` ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS `Next_{{ timestamp_column }}`, first_value(b.`{{ value_column }}`, true) OVER (PARTITION BY a.`{{ tagname_column }}` ORDER BY a.`{{ timestamp_column }}` ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS `Next_{{ value_column }}`, "
        "CASE WHEN b.`{{ value_column }}` is NULL THEN `Last_{{ value_column }}` + (unix_timestamp(a.`{{ timestamp_column }}`) - unix_timestamp(`Last_{{ timestamp_column }}`)) * ((`Next_{{ value_column }}` - `Last_{{ value_column }}`)) / ((unix_timestamp(`Next_{{ timestamp_column }}`) - unix_timestamp(`Last_{{ timestamp_column }}`))) ELSE b.`{{ value_column }}` END AS `linear_interpolated_{{ value_column }}` FROM date_array a FULL OUTER JOIN resample b ON a.`{{ timestamp_column }}` = b.`{{ timestamp_column }}` AND a.`{{ tagname_column }}` = b.`{{ tagname_column }}`) "
        ",project AS (SELECT `{{ timestamp_column }}`, `{{ tagname_column }}`, `linear_interpolated_{{ value_column }}` AS `{{ value_column }}` FROM linear_interpolation_calculations) "
        "{% else %}"
        ",project AS (SELECT * FROM resample) "
        "{% endif %}"
        "{% if pivot is defined and pivot == true %}"
        ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
        "{% for i in range(tag_names | length) %}"
        "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
        "{% endfor %}"
        "))) SELECT * FROM pivot ORDER BY `{{ timestamp_column }}` "
        "{% else %}"
        "SELECT * FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% endif %}"
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    interpolate_parameters = sample_parameters.copy()
    interpolate_parameters["interpolation_method"] = parameters_dict[
        "interpolation_method"
    ]
    if (
        parameters_dict["interpolation_method"] == "forward_fill"
        or parameters_dict["interpolation_method"] == "backward_fill"
    ):
        interpolate_parameters["interpolation_options_0"] = interpolation_options[0]
        interpolate_parameters["interpolation_options_1"] = interpolation_options[1]
        interpolate_parameters["interpolation_options_2"] = interpolation_options[2]

    sql_template = Template(interpolate_query)
    return sql_template.render(interpolate_parameters)


def _interpolation_at_time(parameters_dict: dict) -> str:
    timestamps_deduplicated = list(
        dict.fromkeys(parameters_dict["timestamps"])
    )  # remove potential duplicates in tags
    parameters_dict["timestamps"] = timestamps_deduplicated.copy()
    parameters_dict["min_timestamp"] = min(timestamps_deduplicated)
    parameters_dict["max_timestamp"] = max(timestamps_deduplicated)

    interpolate_at_time_query = (
        "WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`{{ timestamp_column }}`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS `{{ timestamp_column }}`, `{{ tagname_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% else %} 'Good' AS `Status`, {% endif %} `{{ value_column }}` FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE to_date(`{{ timestamp_column }}`) BETWEEN "
        "{% if timestamps is defined %} "
        'date_sub(to_date(to_timestamp("{{ min_timestamp }}")), {{ window_length }}) AND date_add(to_date(to_timestamp("{{ max_timestamp }}")), {{ window_length}}) '
        "{% endif %} AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND `{{ status_column }}` = 'Good' {% endif %}) "
        ", date_array AS (SELECT DISTINCT explode(array( "
        "{% for timestamp in timestamps -%} "
        'from_utc_timestamp(to_timestamp("{{timestamp}}"), "{{time_zone}}") '
        "{% if not loop.last %} , {% endif %} {% endfor %} )) AS `{{ timestamp_column }}`, "
        "explode(array(`{{ tagname_column }}`)) AS `{{ tagname_column }}` FROM raw_events) "
        ", interpolation_events AS (SELECT coalesce(a.`{{ tagname_column }}`, b.`{{ tagname_column }}`) AS `{{ tagname_column }}`, coalesce(a.`{{ timestamp_column }}`, b.`{{ timestamp_column }}`) AS `{{ timestamp_column }}`, a.`{{ timestamp_column }}` AS `Requested_{{ timestamp_column }}`, b.`{{ timestamp_column }}` AS `Found_{{ timestamp_column }}`, b.`{{ status_column }}`, b.`{{ value_column }}` FROM date_array a FULL OUTER JOIN  raw_events b ON a.`{{ timestamp_column }}` = b.`{{ timestamp_column }}` AND a.`{{ tagname_column }}` = b.`{{ tagname_column }}`) "
        ", interpolation_calculations AS (SELECT *, lag(`{{ timestamp_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Previous_{{ timestamp_column }}`, lag(`{{ value_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Previous_{{ value_column }}`, lead(`{{ timestamp_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Next_{{ timestamp_column }}`, lead(`{{ value_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Next_{{ value_column }}`, "
        "CASE WHEN `Requested_{{ timestamp_column }}` = `Found_{{ timestamp_column }}` THEN `{{ value_column }}` WHEN `Next_{{ timestamp_column }}` IS NULL THEN `Previous_{{ value_column }}` WHEN `Previous_{{ timestamp_column }}` IS NULL AND `Next_{{ timestamp_column }}` IS NULL THEN NULL "
        "ELSE `Previous_{{ value_column }}` + ((`Next_{{ value_column }}` - `Previous_{{ value_column }}`) * ((unix_timestamp(`{{ timestamp_column }}`) - unix_timestamp(`Previous_{{ timestamp_column }}`)) / (unix_timestamp(`Next_{{ timestamp_column }}`) - unix_timestamp(`Previous_{{ timestamp_column }}`)))) END AS `Interpolated_{{ value_column }}` FROM interpolation_events) "
        ",project AS (SELECT `{{ tagname_column }}`, `{{ timestamp_column }}`, `Interpolated_{{ value_column }}` AS `{{ value_column }}` FROM interpolation_calculations WHERE `{{ timestamp_column }}` IN ( "
        "{% for timestamp in timestamps -%} "
        'from_utc_timestamp(to_timestamp("{{timestamp}}"), "{{time_zone}}") '
        "{% if not loop.last %} , {% endif %} {% endfor %}) "
        ") "
        "{% if pivot is defined and pivot == true %}"
        ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
        "{% for i in range(tag_names | length) %}"
        "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
        "{% endfor %}"
        "))) SELECT * FROM pivot ORDER BY `{{ timestamp_column }}` "
        "{% else %}"
        "SELECT * FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% endif %}"
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    interpolation_at_time_parameters = {
        "source": parameters_dict.get("source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "timestamps": parameters_dict["timestamps"],
        "include_bad_data": parameters_dict["include_bad_data"],
        "time_zone": parameters_dict["time_zone"],
        "min_timestamp": parameters_dict["min_timestamp"],
        "max_timestamp": parameters_dict["max_timestamp"],
        "window_length": parameters_dict["window_length"],
        "pivot": parameters_dict.get("pivot", None),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "timestamp_column": parameters_dict.get("timestamp_column", "EventTime"),
        "include_status": (
            False
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else True
        ),
        "status_column": (
            "Status"
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else parameters_dict.get("status_column", "Status")
        ),
        "value_column": parameters_dict.get("value_column", "Value"),
    }
    sql_template = Template(interpolate_at_time_query)
    return sql_template.render(interpolation_at_time_parameters)


def _metadata_query(parameters_dict: dict) -> str:
    metadata_query = (
        "SELECT * FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` "
        "{% endif %}"
        "{% if tag_names is defined and tag_names|length > 0 %} "
        "WHERE UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    metadata_parameters = {
        "source": parameters_dict.get("source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
    }

    sql_template = Template(metadata_query)
    return sql_template.render(metadata_parameters)


def _latest_query(parameters_dict: dict) -> str:
    latest_query = (
        "SELECT * FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_latest` "
        "{% endif %}"
        "{% if tag_names is defined and tag_names|length > 0 %} "
        "WHERE UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    latest_parameters = {
        "source": parameters_dict.get("source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
    }

    sql_template = Template(latest_query)
    return sql_template.render(latest_parameters)


def _time_weighted_average_query(parameters_dict: dict) -> str:
    parameters_dict["start_datetime"] = datetime.strptime(
        parameters_dict["start_date"], TIMESTAMP_FORMAT
    ).strftime("%Y-%m-%dT%H:%M:%S")
    parameters_dict["end_datetime"] = datetime.strptime(
        parameters_dict["end_date"], TIMESTAMP_FORMAT
    ).strftime("%Y-%m-%dT%H:%M:%S")

    time_weighted_average_query = (
        "WITH raw_events AS (SELECT DISTINCT `{{ tagname_column }}`, from_utc_timestamp(to_timestamp(date_format(`{{ timestamp_column }}`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS `{{ timestamp_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% else %} 'Good' AS `Status`, {% endif %} `{{ value_column }}` FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE to_date(`{{ timestamp_column }}`) BETWEEN date_sub(to_date(to_timestamp(\"{{ start_date }}\")), {{ window_length }}) AND date_add(to_date(to_timestamp(\"{{ end_date }}\")), {{ window_length }}) AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}')  "
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND `{{ status_column }}` = 'Good' {% endif %}) "
        ',date_array AS (SELECT DISTINCT explode(sequence(from_utc_timestamp(to_timestamp("{{ start_date }}"), "{{ time_zone }}"), from_utc_timestamp(to_timestamp("{{ end_date }}"), "{{ time_zone }}"), INTERVAL \'{{ time_interval_rate + \' \' + time_interval_unit }}\')) AS `{{ timestamp_column }}`, explode(array(`{{ tagname_column }}`)) AS `{{ tagname_column }}` FROM raw_events) '
        ",boundary_events AS (SELECT coalesce(a.`{{ tagname_column }}`, b.`{{ tagname_column }}`) AS `{{ tagname_column }}`, coalesce(a.`{{ timestamp_column }}`, b.`{{ timestamp_column }}`) AS `{{ timestamp_column }}`, b.`{{ status_column }}`, b.`{{ value_column }}` FROM date_array a FULL OUTER JOIN raw_events b ON a.`{{ timestamp_column }}` = b.`{{ timestamp_column }}` AND a.`{{ tagname_column }}` = b.`{{ tagname_column }}`) "
        ",window_buckets AS (SELECT `{{ timestamp_column }}` AS window_start, LEAD(`{{ timestamp_column }}`) OVER (ORDER BY `{{ timestamp_column }}`) AS window_end FROM (SELECT distinct `{{ timestamp_column }}` FROM date_array) ) "
        ",window_events AS (SELECT /*+ RANGE_JOIN(b, {{ range_join_seconds }} ) */ b.`{{ tagname_column }}`, b.`{{ timestamp_column }}`, a.window_start AS `WindowEventTime`, b.`{{ status_column }}`, b.`{{ value_column }}` FROM boundary_events b LEFT OUTER JOIN window_buckets a ON a.window_start <= b.`{{ timestamp_column }}` AND a.window_end > b.`{{ timestamp_column }}`) "
        ',fill_status AS (SELECT *, last_value(`{{ status_column }}`, true) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_{{ status_column }}`, CASE WHEN `Fill_{{ status_column }}` = "Good" THEN `{{ value_column }}` ELSE null END AS `Good_{{ value_column }}` FROM window_events) '
        ",fill_value AS (SELECT *, last_value(`Good_{{ value_column }}`, true) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_{{ value_column }}` FROM fill_status) "
        '{% if step is defined and step == "metadata" %} '
        ",fill_step AS (SELECT *, IFNULL(Step, false) AS Step FROM fill_value f LEFT JOIN "
        "{% if source_metadata is defined and source_metadata is not none %}"
        "`{{ source_metadata|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` "
        "{% endif %}"
        "m ON f.`{{ tagname_column }}` = m.`{{ tagname_column }}`) "
        "{% else %}"
        ",fill_step AS (SELECT *, {{ step }} AS Step FROM fill_value) "
        "{% endif %}"
        ",interpolate AS (SELECT *, CASE WHEN `Step` = false AND `{{ status_column }}` IS NULL AND `{{ value_column }}` IS NULL THEN lag(`{{ timestamp_column }}`) OVER ( PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ) ELSE NULL END AS `Previous_{{ timestamp_column }}`, CASE WHEN `Step` = false AND `{{ status_column }}` IS NULL AND `{{ value_column }}` IS NULL THEN lag(`Fill_{{ value_column }}`) OVER ( PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ) ELSE NULL END AS `Previous_Fill_{{ value_column }}`, "
        "lead(`{{ timestamp_column }}`) OVER ( PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ) AS `Next_{{ timestamp_column }}`, CASE WHEN `Step` = false AND `Status` IS NULL AND `{{ value_column }}` IS NULL THEN lead(`Fill_{{ value_column }}`) OVER ( PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ) ELSE NULL END AS `Next_Fill_{{ value_column }}`, CASE WHEN `Step` = false AND `{{ status_column }}` IS NULL AND `{{ value_column }}` IS NULL THEN `Previous_Fill_{{ value_column }}` + ( (`Next_Fill_{{ value_column }}` - `Previous_Fill_{{ value_column }}`) * ( ( unix_timestamp(`{{ timestamp_column }}`) - unix_timestamp(`Previous_{{ timestamp_column }}`) ) / ( unix_timestamp(`Next_{{ timestamp_column }}`) - unix_timestamp(`Previous_{{ timestamp_column }}`) ) ) ) ELSE NULL END AS `Interpolated_{{ value_column }}`, coalesce(`Interpolated_{{ value_column }}`, `Fill_{{ value_column }}`) as `Event_{{ value_column }}` FROM fill_step )"
        ",twa_calculations AS (SELECT `{{ tagname_column }}`, `{{ timestamp_column }}`, `Window{{ timestamp_column }}`, `Step`, `{{ status_column }}`, `{{ value_column }}`, `Previous_{{ timestamp_column }}`, `Previous_Fill_{{ value_column }}`, `Next_{{ timestamp_column }}`, `Next_Fill_{{ value_column }}`, `Interpolated_{{ value_column }}`, `Fill_{{ status_column }}`, `Fill_{{ value_column }}`, `Event_{{ value_column }}`, lead(`Fill_{{ status_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Next_{{ status_column }}` "
        ', CASE WHEN `Next_{{ status_column }}` = "Good" OR (`Fill_{{ status_column }}` = "Good" AND `Next_{{ status_column }}` != "Good") THEN lead(`Event_{{ value_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) ELSE `{{ value_column }}` END AS `Next_{{ value_column }}_For_{{ status_column }}` '
        ', CASE WHEN `Fill_{{ status_column }}` = "Good" THEN `Next_{{ value_column }}_For_{{ status_column }}` ELSE 0 END AS `Next_{{ value_column }}` '
        ', CASE WHEN `Fill_{{ status_column }}` = "Good" AND `Next_{{ status_column }}` = "Good" THEN ((cast(`Next_{{ timestamp_column }}` AS double) - cast(`{{ timestamp_column }}` AS double)) / 60) WHEN `Fill_{{ status_column }}` = "Good" AND `Next_{{ status_column }}` != "Good" THEN ((cast(`Next_{{ timestamp_column }}` AS integer) - cast(`{{ timestamp_column }}` AS double)) / 60) ELSE 0 END AS good_minutes '
        ", CASE WHEN Step == false THEN ((`Event_{{ value_column }}` + `Next_{{ value_column }}`) * 0.5) * good_minutes ELSE (`Event_{{ value_column }}` * good_minutes) END AS twa_value FROM interpolate) "
        ",twa AS (SELECT `{{ tagname_column }}`, `Window{{ timestamp_column }}` AS `{{ timestamp_column }}`, sum(twa_value) / sum(good_minutes) AS `{{ value_column }}` from twa_calculations GROUP BY `{{ tagname_column }}`, `Window{{ timestamp_column }}`) "
        ',project AS (SELECT * FROM twa WHERE `{{ timestamp_column }}` BETWEEN to_timestamp("{{ start_datetime }}") AND to_timestamp("{{ end_datetime }}")) '
        "{% if pivot is defined and pivot == true %}"
        ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
        "{% for i in range(tag_names | length) %}"
        "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
        "{% endfor %}"
        "))) SELECT * FROM pivot ORDER BY `{{ timestamp_column }}` "
        "{% else %}"
        "SELECT * FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% endif %}"
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    time_weighted_average_parameters = {
        "source": parameters_dict.get("source", None),
        "source_metadata": parameters_dict.get("source_metadata", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "start_datetime": parameters_dict["start_datetime"],
        "end_datetime": parameters_dict["end_datetime"],
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "time_interval_rate": parameters_dict["time_interval_rate"],
        "time_interval_unit": parameters_dict["time_interval_unit"],
        "window_length": parameters_dict["window_length"],
        "include_bad_data": parameters_dict["include_bad_data"],
        "step": parameters_dict["step"],
        "pivot": parameters_dict.get("pivot", None),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "time_zone": parameters_dict["time_zone"],
        "range_join_seconds": parameters_dict["range_join_seconds"],
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "timestamp_column": parameters_dict.get("timestamp_column", "EventTime"),
        "include_status": (
            False
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else True
        ),
        "status_column": (
            "Status"
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else parameters_dict.get("status_column", "Status")
        ),
        "value_column": parameters_dict.get("value_column", "Value"),
    }

    sql_template = Template(time_weighted_average_query)
    return sql_template.render(time_weighted_average_parameters)


def _circular_stats_query(parameters_dict: dict) -> str:
    circular_base_query = (
        "WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`{{ timestamp_column }}`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"{{ time_zone }}\") AS `{{ timestamp_column }}`, `{{ tagname_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% else %} 'Good' AS `Status`, {% endif %} `{{ value_column }}` FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE `{{ timestamp_column }}` BETWEEN TO_TIMESTAMP(\"{{ start_date }}\") AND TO_TIMESTAMP(\"{{ end_date }}\") AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND `{{ status_column }}` = 'Good' {% endif %}) "
        ',date_array AS (SELECT DISTINCT EXPLODE(SEQUENCE(FROM_UTC_TIMESTAMP(TO_TIMESTAMP("{{ start_date }}"), "{{ time_zone }}"), FROM_UTC_TIMESTAMP(TO_TIMESTAMP("{{ end_date }}"), "{{ time_zone }}"), INTERVAL \'{{ time_interval_rate + \' \' + time_interval_unit }}\')) AS `{{ timestamp_column }}`, EXPLODE(ARRAY(`{{ tagname_column }}`)) AS `{{ tagname_column }}` FROM raw_events)  '
        ",window_events AS (SELECT COALESCE(a.`{{ tagname_column }}`, b.`{{ tagname_column }}`) AS `{{ tagname_column }}`, COALESCE(a.`{{ timestamp_column }}`, b.`{{ timestamp_column }}`) AS `{{ timestamp_column }}`, WINDOW(COALESCE(a.`{{ timestamp_column }}`, b.`{{ timestamp_column }}`), '{{ time_interval_rate + ' ' + time_interval_unit }}').START `Window{{ timestamp_column }}`, b.`{{ status_column }}`, b.`{{ value_column }}` FROM date_array a FULL OUTER JOIN raw_events b ON CAST(a.`{{ timestamp_column }}` AS LONG) = CAST(b.`{{ timestamp_column }}` AS LONG) AND a.`{{ tagname_column }}` = b.`{{ tagname_column }}`) "
        ",calculation_set_up AS (SELECT `{{ timestamp_column }}`, `Window{{ timestamp_column }}`, `{{ tagname_column }}`, `{{ value_column }}`, MOD(`{{ value_column }}` - {{ lower_bound }}, ({{ upper_bound }} - {{ lower_bound }}))*(2*pi()/({{ upper_bound }} - {{ lower_bound }})) AS `{{ value_column }}_in_Radians`, LAG(`{{ timestamp_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Previous_{{ timestamp_column }}`, (unix_millis(`{{ timestamp_column }}`) - unix_millis(`Previous_{{ timestamp_column }}`)) / 86400000 AS Time_Difference, COS(`{{ value_column }}_in_Radians`) AS Cos_Value, SIN(`{{ value_column }}_in_Radians`) AS Sin_Value FROM window_events) "
        ",circular_average_calculations AS (SELECT `Window{{ timestamp_column }}`, `{{ tagname_column }}`, Time_Difference, AVG(Cos_Value) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Average_Cos, AVG(Sin_Value) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Average_Sin, SQRT(POW(Average_Cos, 2) + POW(Average_Sin, 2)) AS Vector_Length, Average_Cos/Vector_Length AS Rescaled_Average_Cos, Average_Sin/Vector_Length AS Rescaled_Average_Sin, Time_Difference * Rescaled_Average_Cos AS Diff_Average_Cos, Time_Difference * Rescaled_Average_Sin AS Diff_Average_Sin FROM calculation_set_up) "
    )

    if parameters_dict["circular_function"] == "average":
        circular_stats_query = (
            f"{circular_base_query} "
            ",circular_average_results AS (SELECT `Window{{ timestamp_column }}` AS `{{ timestamp_column }}`, `{{ tagname_column }}`, sum(Diff_Average_Cos)/sum(Time_Difference) AS Cos_Time_Averages, sum(Diff_Average_Sin)/sum(Time_Difference) AS Sin_Time_Averages, array_min(array(1, sqrt(pow(Cos_Time_Averages, 2) + pow(Sin_Time_Averages, 2)))) AS R, mod(2*pi() + atan2(Sin_Time_Averages, Cos_Time_Averages), 2*pi()) AS Circular_Average_Value_in_Radians, (Circular_Average_Value_in_Radians * ({{ upper_bound }} - {{ lower_bound }})) / (2*pi())+ 0 AS Circular_Average_Value_in_Degrees FROM circular_average_calculations GROUP BY `{{ tagname_column }}`, `Window{{ timestamp_column }}`) "
            ",project AS (SELECT `{{ timestamp_column }}`, `{{ tagname_column }}`, Circular_Average_Value_in_Degrees AS `{{ value_column }}` FROM circular_average_results) "
            "{% if pivot is defined and pivot == true %}"
            ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
            "{% for i in range(tag_names | length) %}"
            "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
            "{% endfor %}"
            "))) SELECT * FROM pivot ORDER BY `{{ timestamp_column }}` "
            "{% else %}"
            "SELECT * FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
            "{% endif %}"
            "{% if limit is defined and limit is not none %}"
            "LIMIT {{ limit }} "
            "{% endif %}"
            "{% if offset is defined and offset is not none %}"
            "OFFSET {{ offset }} "
            "{% endif %}"
        )
    elif parameters_dict["circular_function"] == "standard_deviation":
        circular_stats_query = (
            f"{circular_base_query} "
            ",circular_average_results AS (SELECT `Window{{ timestamp_column }}` AS `{{ timestamp_column }}`, `{{ tagname_column }}`, sum(Diff_Average_Cos)/sum(Time_Difference) AS Cos_Time_Averages, sum(Diff_Average_Sin)/sum(Time_Difference) AS Sin_Time_Averages, array_min(array(1, sqrt(pow(Cos_Time_Averages, 2) + pow(Sin_Time_Averages, 2)))) AS R, mod(2*pi() + atan2(Sin_Time_Averages, Cos_Time_Averages), 2*pi()) AS Circular_Average_Value_in_Radians, SQRT(-2*LN(R)) * ( {{ upper_bound }} - {{ lower_bound }}) / (2*PI()) AS Circular_Standard_Deviation FROM circular_average_calculations GROUP BY `{{ tagname_column }}`, `Window{{ timestamp_column }}`) "
            ",project AS (SELECT `{{ timestamp_column }}`, `{{ tagname_column }}`, Circular_Standard_Deviation AS `Value` FROM circular_average_results) "
            "{% if pivot is defined and pivot == true %}"
            ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
            "{% for i in range(tag_names | length) %}"
            "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
            "{% endfor %}"
            "))) SELECT * FROM pivot ORDER BY `{{ timestamp_column }}` "
            "{% else %}"
            "SELECT * FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
            "{% endif %}"
            "{% if limit is defined and limit is not none %}"
            "LIMIT {{ limit }} "
            "{% endif %}"
            "{% if offset is defined and offset is not none %}"
            "OFFSET {{ offset }} "
            "{% endif %}"
        )

    circular_stats_parameters = {
        "source": parameters_dict.get("source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "time_interval_rate": parameters_dict["time_interval_rate"],
        "time_interval_unit": parameters_dict["time_interval_unit"],
        "lower_bound": parameters_dict["lower_bound"],
        "upper_bound": parameters_dict["upper_bound"],
        "include_bad_data": parameters_dict["include_bad_data"],
        "time_zone": parameters_dict["time_zone"],
        "circular_function": parameters_dict["circular_function"],
        "pivot": parameters_dict.get("pivot", None),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "timestamp_column": parameters_dict.get("timestamp_column", "EventTime"),
        "include_status": (
            False
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else True
        ),
        "status_column": (
            "Status"
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else parameters_dict.get("status_column", "Status")
        ),
        "value_column": parameters_dict.get("value_column", "Value"),
    }

    sql_template = Template(circular_stats_query)
    return sql_template.render(circular_stats_parameters)


def _summary_query(parameters_dict: dict) -> str:
    summary_query = (
        "SELECT `{{ tagname_column }}`, "
        "count(`{{ value_column }}`) as Count, "
        "CAST(Avg(`{{ value_column }}`) as decimal(10, 2)) as Avg, "
        "CAST(Min(`{{ value_column }}`) as decimal(10, 2)) as Min, "
        "CAST(Max(`{{ value_column }}`) as decimal(10, 2)) as Max, "
        "CAST(stddev(`{{ value_column }}`) as decimal(10, 2)) as StDev, "
        "CAST(sum(`{{ value_column }}`) as decimal(10, 2)) as Sum, "
        "CAST(variance(`{{ value_column }}`) as decimal(10, 2)) as Var FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE `{{ timestamp_column }}` BETWEEN to_timestamp(\"{{ start_date }}\") AND to_timestamp(\"{{ end_date }}\") AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %}"
        "AND `{{ status_column }}` = 'Good'"
        "{% endif %}"
        "GROUP BY `{{ tagname_column }}` "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    summary_parameters = {
        "source": parameters_dict.get("source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "include_bad_data": parameters_dict["include_bad_data"],
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "time_zone": parameters_dict["time_zone"],
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "timestamp_column": parameters_dict.get("timestamp_column", "EventTime"),
        "include_status": (
            False
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else True
        ),
        "status_column": (
            "Status"
            if "status_column" in parameters_dict
            and parameters_dict.get("status_column") is None
            else parameters_dict.get("status_column", "Status")
        ),
        "value_column": parameters_dict.get("value_column", "Value"),
    }

    sql_template = Template(summary_query)
    return sql_template.render(summary_parameters)


def _query_builder(parameters_dict: dict, query_type: str) -> str:
    if "supress_warning" not in parameters_dict:
        logging.warning(
            "Please use the TimeSeriesQueryBuilder() to build time series queries."
        )

    if "tag_names" not in parameters_dict:
        parameters_dict["tag_names"] = []
    tagnames_deduplicated = list(
        dict.fromkeys(parameters_dict["tag_names"])
    )  # remove potential duplicates in tags
    parameters_dict["tag_names"] = tagnames_deduplicated.copy()

    if query_type == "metadata":
        return _metadata_query(parameters_dict)

    if query_type == "latest":
        return _latest_query(parameters_dict)

    parameters_dict = _parse_dates(parameters_dict)

    if query_type == "interpolation_at_time":
        return _interpolation_at_time(parameters_dict)

    if query_type == "raw":
        return _raw_query(parameters_dict)

    if query_type == "resample":
        parameters_dict["range_join_seconds"] = _convert_to_seconds(
            parameters_dict["time_interval_rate"]
            + " "
            + parameters_dict["time_interval_unit"][0]
        )
        sample_prepared_query, sample_query, sample_parameters = _sample_query(
            parameters_dict
        )
        return sample_prepared_query

    if query_type == "interpolate":
        parameters_dict["range_join_seconds"] = _convert_to_seconds(
            parameters_dict["time_interval_rate"]
            + " "
            + parameters_dict["time_interval_unit"][0]
        )
        sample_prepared_query, sample_query, sample_parameters = _sample_query(
            parameters_dict
        )
        sample_parameters["is_resample"] = False
        return _interpolation_query(parameters_dict, sample_query, sample_parameters)

    if query_type == "time_weighted_average":
        parameters_dict["range_join_seconds"] = _convert_to_seconds(
            parameters_dict["time_interval_rate"]
            + " "
            + parameters_dict["time_interval_unit"][0]
        )
        return _time_weighted_average_query(parameters_dict)

    if query_type == "circular_average":
        parameters_dict["circular_function"] = "average"
        return _circular_stats_query(parameters_dict)

    if query_type == "circular_standard_deviation":
        parameters_dict["circular_function"] = "standard_deviation"
        return _circular_stats_query(parameters_dict)

    if query_type == "summary":
        return _summary_query(parameters_dict)
