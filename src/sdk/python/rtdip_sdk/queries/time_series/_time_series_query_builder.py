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


def _build_sql_cte_statement(sql_query_list):
    sql_cte_query = ", ".join(
        [sql_query["sql_query"] for sql_query in sql_query_list[:-1]],
    )

    sql_cte_query = " ".join(["WITH", sql_cte_query])

    if len(sql_cte_query) > 1:
        sql_cte_query = " ".join([sql_cte_query, sql_query_list[-1]["sql_query"]])

    return sql_cte_query


def _window_start_time_offset(start_date, time_interval_rate, time_interval_unit: str):
    time_interval_rate_number = float(time_interval_rate)

    if "day" in time_interval_unit:
        time_interval_rate_seconds = time_interval_rate_number * 24 * 3600
    elif "hour" in time_interval_unit:
        time_interval_rate_seconds = time_interval_rate_number * 3600
    elif "minute" in time_interval_unit:
        time_interval_rate_seconds = time_interval_rate_number * 60
    elif "second" in time_interval_unit:
        time_interval_rate_seconds = time_interval_rate_number

    # Calculate Offset for startTime parameter

    offset_start_time = (
        datetime.strptime(start_date, TIMESTAMP_FORMAT).timestamp()
        % time_interval_rate_seconds
    )

    offset_start_time = f"{int(offset_start_time)} second"
    return offset_start_time


def _build_raw_query(
    sql_query_name,
    timestamp_column,
    tagname_column,
    status_column,
    value_column,
    start_date,
    end_date,
    time_zone,
    time_interval_rate=None,
    time_interval_unit=None,
    agg_method=None,
    deduplicate=None,
    source=None,
    business_unit=None,
    asset=None,
    data_security_level=None,
    data_type=None,
    tag_names=None,
    include_status=None,
    include_bad_data=None,
    case_insensitivity_tag_search=None,
    sort=True,
):
    # Select
    raw_query_sql = f"{sql_query_name} AS (SELECT"
    if agg_method == "avg" or deduplicate == True:
        raw_query_sql = " ".join([raw_query_sql, "DISTINCT"])

    # Event Time
    raw_query_sql = " ".join(
        [
            raw_query_sql,
            f"from_utc_timestamp(date_trunc('millisecond',`{timestamp_column}`), '{time_zone}') AS `{timestamp_column}`,",
        ]
    )
    if time_interval_rate is not None:
        window_offset_start_time = _window_start_time_offset(
            start_date=start_date,
            time_interval_rate=time_interval_rate,
            time_interval_unit=time_interval_unit,
        )
        raw_query_sql = " ".join(
            [
                raw_query_sql,
                f"window(from_utc_timestamp(date_trunc('millisecond',`{timestamp_column}`), '{time_zone}'), '{time_interval_rate} {time_interval_unit}', '{time_interval_rate} {time_interval_unit}', '{window_offset_start_time}') AS `window`,",
            ]
        )

    # Tag Name
    raw_query_sql = " ".join([raw_query_sql, f"`{tagname_column}`,"])

    # Status
    if include_status == True:
        raw_query_sql = " ".join([raw_query_sql, f"`{status_column}`,"])
    else:
        raw_query_sql = " ".join([raw_query_sql, "'Good' AS `Status`,"])

    # Value
    raw_query_sql = " ".join([raw_query_sql, f"`{value_column}` FROM"])

    if source is not None:
        raw_query_sql = " ".join([raw_query_sql, f"`{source.lower()}`"])
    else:
        raw_query_sql = " ".join(
            [
                raw_query_sql,
                f"`{business_unit.lower()}`.`sensors`.`{asset.lower()}_{data_security_level.lower()}_events_{data_type.lower()}`",
            ]
        )

    # Where
    to_timestamp = (
        f"to_timestamp('{end_date}')"
        if time_interval_rate is None
        else f"timestampadd({time_interval_unit}, {time_interval_rate}, to_timestamp('{end_date}'))"
    )

    raw_query_sql = " ".join(
        [
            raw_query_sql,
            f"WHERE `{timestamp_column}` BETWEEN to_timestamp('{start_date}') AND {to_timestamp} AND",
        ]
    )

    if case_insensitivity_tag_search == True:
        quoted_tag_names = "', '".join([tag.upper() for tag in tag_names])
        raw_query_sql = " ".join(
            [
                raw_query_sql,
                f"UPPER(`{tagname_column}`) IN ('{quoted_tag_names}')",
            ]
        )
    else:
        quoted_tag_names = "', '".join(tag_names)
        raw_query_sql = " ".join(
            [
                raw_query_sql,
                f"`{tagname_column}` IN ('{quoted_tag_names}')",
            ]
        )

    if include_status == True and include_bad_data == False:
        raw_query_sql = " ".join([raw_query_sql, f"AND `{status_column}` <> 'Bad'"])

    if sort == True:
        raw_query_sql = " ".join(
            [
                raw_query_sql,
                f"ORDER BY `{tagname_column}`, `{timestamp_column}`",
            ]
        )
    raw_query_sql += ")"

    return raw_query_sql


def _build_resample_query(
    sql_query_list,
    sql_query_name,
    timestamp_column,
    tagname_column,
    value_column,
    tag_names,
    start_date,
    end_date,
    time_zone,
    time_interval_rate,
    time_interval_unit,
    agg_method,
    case_insensitivity_tag_search,
    fill=False,
    sort=True,
):
    parent_sql_query_name = sql_query_list[-1]["query_name"]

    from_sql = parent_sql_query_name
    timestamp_sql = f"{parent_sql_query_name}.`window`.start"
    tagname_sql = f"{parent_sql_query_name}.`{tagname_column}`"
    groupby_sql = f"{parent_sql_query_name}.`{tagname_column}`, {parent_sql_query_name}.`window`.start"

    if fill == True:
        quoted_tag_names = (
            "', '".join([tag.upper() for tag in tag_names])
            if case_insensitivity_tag_search == True
            else "', '".join(tag_names)
        )
        date_fill_query = f"fill_intervals AS (SELECT DISTINCT explode(sequence(from_utc_timestamp(to_timestamp('{start_date}'), '{time_zone}'), from_utc_timestamp(to_timestamp('{end_date}'), '{time_zone}'), INTERVAL '{time_interval_rate} {time_interval_unit}')) AS `{timestamp_column}`, explode(array('{quoted_tag_names}')) AS `{tagname_column}`)"
        from_sql = f"fill_intervals LEFT OUTER JOIN {parent_sql_query_name} ON fill_intervals.`{timestamp_column}` = {parent_sql_query_name}.`window`.start AND fill_intervals.`{tagname_column}` = {parent_sql_query_name}.`{tagname_column}`"
        timestamp_sql = f"fill_intervals.`{timestamp_column}`"
        tagname_sql = f"fill_intervals.`{tagname_column}`"
        groupby_sql = (
            f"fill_intervals.`{tagname_column}`, fill_intervals.`{timestamp_column}`"
        )

    resample_query_sql = f"{sql_query_name} AS (SELECT {tagname_sql}, {timestamp_sql} AS `{timestamp_column}`, {agg_method}({parent_sql_query_name}.`{value_column}`) AS `{value_column}` FROM {from_sql} GROUP BY {groupby_sql}"

    if fill == True:
        resample_query_sql = ", ".join(
            [
                date_fill_query,
                resample_query_sql,
            ]
        )

    if sort == True:
        resample_query_sql = " ".join(
            [
                resample_query_sql,
                f"ORDER BY `{tagname_column}`, `{timestamp_column}`",
            ]
        )

    return resample_query_sql + ")"


def _build_fill_intervals_query(
    sql_query_list,
    sql_query_name,
    timestamp_column,
    tagname_column,
    value_column,
    tag_names,
    start_date,
    end_date,
    time_zone,
    time_interval_rate,
    time_interval_unit,
    case_insensitivity_tag_search,
):
    parent_sql_query_name = sql_query_list[-1]["query_name"]
    quoted_tag_names = (
        "', '".join([tag.upper() for tag in tag_names])
        if case_insensitivity_tag_search == True
        else "', '".join(tag_names)
    )
    intervals_query = f"intervals AS (SELECT DISTINCT explode(sequence(from_utc_timestamp(to_timestamp('{start_date}'), '{time_zone}'), from_utc_timestamp(to_timestamp('{end_date}'), '{time_zone}'), INTERVAL '{time_interval_rate} {time_interval_unit}')) AS `{timestamp_column}`, explode(array('{quoted_tag_names}')) AS `{tagname_column}`), "
    fill_intervals_query = f"{sql_query_name} as (SELECT intervals.`{tagname_column}`, intervals.`{timestamp_column}` as `{timestamp_column}`, raw. `{timestamp_column}` as `Original{timestamp_column}`, raw.`{value_column}`, CASE WHEN raw.`{value_column}` IS NULL THEN NULL ELSE struct(raw.`{timestamp_column}`, raw.`{value_column}`) END AS `{timestamp_column}_{value_column}` "
    from_sql = f"FROM intervals LEFT OUTER JOIN {parent_sql_query_name} ON intervals.`{timestamp_column}` = {parent_sql_query_name}.`window`.start AND intervals.`{tagname_column}` = {parent_sql_query_name}.`{tagname_column}`"

    return intervals_query + fill_intervals_query + from_sql + ")"


def _build_interpolate_query(
    sql_query_list,
    sql_query_name,
    tagname_column,
    timestamp_column,
    value_column,
    sort=True,
):
    parent_sql_query_name = sql_query_list[-1]["query_name"]

    interpolate_calc_query_sql = f"{sql_query_name}_calculate AS (SELECT `Original{timestamp_column}`, `{timestamp_column}`, `{tagname_column}`, "
    lag_value_query_sql = f"CASE WHEN `{value_column}` IS NOT NULL THEN NULL ELSE LAG(`{timestamp_column}_{value_column}`) IGNORE NULLS OVER (PARTITION BY `{tagname_column}` ORDER BY `{timestamp_column}`) END AS Prev{timestamp_column}{value_column}, "
    lead_value_query_sql = f"CASE WHEN `{value_column}` IS NOT NULL THEN NULL ELSE LEAD(`{timestamp_column}_{value_column}`) IGNORE NULLS OVER (PARTITION BY `{tagname_column}` ORDER BY `{timestamp_column}`) END AS Next{timestamp_column}{value_column}, "
    value_query_sql = f"CASE WHEN `Original{timestamp_column}` = `{timestamp_column}` THEN `{value_column}` WHEN `Prev{timestamp_column}{value_column}` IS NOT NULL AND `Next{timestamp_column}{value_column}` IS NOT NULL THEN `Prev{timestamp_column}{value_column}`.`{value_column}` + ((`Next{timestamp_column}{value_column}`.`{value_column}` - `Prev{timestamp_column}{value_column}`.`{value_column}`) * (unix_timestamp(`{timestamp_column}`) - unix_timestamp(`Prev{timestamp_column}{value_column}`.`{timestamp_column}`)) / (unix_timestamp(`Next{timestamp_column}{value_column}`.`{timestamp_column}`) - unix_timestamp(`Prev{timestamp_column}{value_column}`.`{timestamp_column}`))) WHEN `Prev{timestamp_column}{value_column}` IS NOT NULL THEN `Prev{timestamp_column}{value_column}`.`{value_column}` ELSE NULL END as `{value_column}` FROM {parent_sql_query_name} "
    interpolate_project_query_sql = f"), {sql_query_name} AS (SELECT `{timestamp_column}`, `{tagname_column}`, `{value_column}` FROM {sql_query_name}_calculate WHERE `Original{timestamp_column}` IS NULL OR `Original{timestamp_column}` = `{timestamp_column}` "

    interpolate_query_sql = (
        interpolate_calc_query_sql
        + lag_value_query_sql
        + lead_value_query_sql
        + value_query_sql
        + interpolate_project_query_sql
    )

    if sort == True:
        interpolate_query_sql = " ".join(
            [
                interpolate_query_sql,
                f"ORDER BY `{tagname_column}`, `{timestamp_column}`",
            ]
        )

    return interpolate_query_sql + ")"


def _build_summary_query(
    sql_query_name,
    timestamp_column,
    tagname_column,
    status_column,
    value_column,
    start_date,
    end_date,
    source=None,
    business_unit=None,
    asset=None,
    data_security_level=None,
    data_type=None,
    tag_names=None,
    include_status=None,
    include_bad_data=None,
    case_insensitivity_tag_search=None,
):

    # Select
    summary_query_sql = f"{sql_query_name} AS (SELECT `{tagname_column}`, "
    summary_query_sql = " ".join(
        [
            summary_query_sql,
            f"count(`{value_column}`) as Count,",
            f"CAST(Avg(`{value_column}`) as decimal(10, 2)) as Avg,",
            f"CAST(Min(`{value_column}`) as decimal(10, 2)) as Min,",
            f"CAST(Max(`{value_column}`) as decimal(10, 2)) as Max,",
            f"CAST(stddev(`{value_column}`) as decimal(10, 2)) as StDev,",
            f"CAST(sum(`{value_column}`) as decimal(10, 2)) as Sum,",
            f"CAST(variance(`{value_column}`) as decimal(10, 2)) as Var FROM",
        ]
    )

    # From
    if source is not None:
        summary_query_sql = " ".join([summary_query_sql, f"`{source.lower()}`"])
    else:
        summary_query_sql = " ".join(
            [
                summary_query_sql,
                f"`{business_unit.lower()}`.`sensors`.`{asset.lower()}_{data_security_level.lower()}_events_{data_type.lower()}`",
            ]
        )

    # Where EventTime
    summary_query_sql = " ".join(
        [
            summary_query_sql,
            f"WHERE `{timestamp_column}` BETWEEN to_timestamp('{start_date}') AND to_timestamp('{end_date}') AND",
        ]
    )

    # TagName
    if case_insensitivity_tag_search == True:
        quoted_tag_names = "', '".join([tag.upper() for tag in tag_names])
        summary_query_sql = " ".join(
            [
                summary_query_sql,
                f"UPPER(`{tagname_column}`) IN ('{quoted_tag_names}')",
            ]
        )
    else:
        quoted_tag_names = "', '".join(tag_names)
        summary_query_sql = " ".join(
            [summary_query_sql, f"`{tagname_column}` IN ('{quoted_tag_names}')"]
        )

    # Optional bad data filtering
    if include_status == True and include_bad_data == False:
        summary_query_sql = " ".join(
            [summary_query_sql, f"AND `{status_column}` <> 'Bad'"]
        )

    # Group by
    summary_query_sql = " ".join([summary_query_sql, f"GROUP BY `{tagname_column}`"])
    summary_query_sql += ")"

    return summary_query_sql


def _build_pivot_query(
    sql_query_list,
    sql_query_name,
    tagname_column,
    timestamp_column,
    value_column,
    tag_names,
    is_case_insensitive_tag_search,
    sort=True,
):
    parent_sql_query_name = sql_query_list[-1]["query_name"]

    tag_names_string = (
        ", ".join([f"'{tag.upper()}' AS `{tag}`" for tag in tag_names])
        if is_case_insensitive_tag_search == True
        else ", ".join([f"'{tag}' AS `{tag}`" for tag in tag_names])
    )

    pivot_query_sql = f"{sql_query_name} AS (SELECT * FROM (SELECT `{timestamp_column}`, `{value_column}`,"

    if is_case_insensitive_tag_search == True:
        pivot_query_sql = " ".join(
            [pivot_query_sql, f"UPPER(`{tagname_column}`) AS `{tagname_column}`"]
        )
    else:
        pivot_query_sql = " ".join([pivot_query_sql, f"`{tagname_column}`"])

    pivot_query_sql = " ".join(
        [
            pivot_query_sql,
            f"FROM {parent_sql_query_name}) PIVOT (FIRST(`{value_column}`) FOR `{tagname_column}` IN ({tag_names_string}))",
        ]
    )

    if sort == True:
        pivot_query_sql = " ".join(
            [
                pivot_query_sql,
                f"ORDER BY `{timestamp_column}`",
            ]
        )

    return pivot_query_sql + ")"


def _build_uom_query(
    sql_query_list,
    sql_query_name,
    metadata_source,
    business_unit,
    asset,
    data_security_level,
    tagname_column,
    metadata_tagname_column,
    metadata_uom_column,
):
    parent_sql_query_name = sql_query_list[-1]["query_name"]

    uom_sql_query = f"{sql_query_name} AS (SELECT {parent_sql_query_name}.*, metadata.`{metadata_uom_column}` FROM {parent_sql_query_name} LEFT OUTER JOIN"

    if metadata_source:
        uom_sql_query = " ".join([uom_sql_query, f"{metadata_source}"])
    else:
        uom_sql_query = " ".join(
            [
                uom_sql_query,
                f"`{business_unit.lower()}`.`sensors`.`{asset.lower()}_{data_security_level.lower()}_metadata`",
            ]
        )

    uom_sql_query = " ".join(
        [
            uom_sql_query,
            f"AS metadata ON {parent_sql_query_name}.`{tagname_column}` = metadata.`{metadata_tagname_column}`",
        ]
    )

    return uom_sql_query + ")"


def _build_output_query(sql_query_list, to_json, limit, offset):
    parent_sql_query_name = sql_query_list[-1]["query_name"]

    output_sql_query = f"SELECT"

    if to_json == True:
        output_sql_query = " ".join(
            [
                output_sql_query,
                "to_json(struct(*), map('timestampFormat', "
                "'yyyy-MM-dd\\'T\\'HH:mm:ss.SSSSSSSSSXXX'"
                ")) AS Value",
            ]
        )
    else:
        output_sql_query = " ".join([output_sql_query, "*"])

    output_sql_query = " ".join([output_sql_query, f"FROM {parent_sql_query_name}"])

    if limit is not None:
        output_sql_query = " ".join([output_sql_query, f"LIMIT {limit}"])

    if offset is not None:
        output_sql_query = " ".join([output_sql_query, f"OFFSET {offset}"])

    return output_sql_query


def _raw_query(parameters_dict: dict) -> str:

    sql_query_list = []

    raw_parameters = {
        "source": parameters_dict.get("source", None),
        "metadata_source": parameters_dict.get("metadata_source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "include_bad_data": parameters_dict["include_bad_data"],
        "sort": parameters_dict.get("sort", True),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "display_uom": parameters_dict.get("display_uom", False),
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
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "metadata_tagname_column": parameters_dict.get(
            "metadata_tagname_column", "TagName"
        ),
        "metadata_uom_column": parameters_dict.get("metadata_uom_column", "UoM"),
        "to_json": parameters_dict.get("to_json", False),
    }

    raw_query = _build_raw_query(
        sql_query_name="raw",
        timestamp_column=raw_parameters["timestamp_column"],
        tagname_column=raw_parameters["tagname_column"],
        status_column=raw_parameters["status_column"],
        value_column=raw_parameters["value_column"],
        start_date=raw_parameters["start_date"],
        end_date=raw_parameters["end_date"],
        time_zone=raw_parameters["time_zone"],
        deduplicate=True,
        source=raw_parameters["source"],
        business_unit=raw_parameters["business_unit"],
        asset=raw_parameters["asset"],
        data_security_level=raw_parameters["data_security_level"],
        data_type=raw_parameters["data_type"],
        tag_names=raw_parameters["tag_names"],
        include_status=raw_parameters["include_status"],
        case_insensitivity_tag_search=raw_parameters["case_insensitivity_tag_search"],
        sort=raw_parameters["sort"],
    )

    sql_query_list.append({"query_name": "raw", "sql_query": raw_query})

    if raw_parameters["display_uom"] == True:
        uom_query = _build_uom_query(
            sql_query_list=sql_query_list,
            sql_query_name="uom",
            metadata_source=raw_parameters["metadata_source"],
            business_unit=raw_parameters["business_unit"],
            asset=raw_parameters["asset"],
            data_security_level=raw_parameters["data_security_level"],
            tagname_column=raw_parameters["tagname_column"],
            metadata_tagname_column=raw_parameters["metadata_tagname_column"],
            metadata_uom_column=raw_parameters["metadata_uom_column"],
        )

        sql_query_list.append({"query_name": "uom", "sql_query": uom_query})

    output_query = _build_output_query(
        sql_query_list=sql_query_list,
        to_json=raw_parameters["to_json"],
        limit=raw_parameters["limit"],
        offset=raw_parameters["offset"],
    )

    sql_query_list.append({"query_name": "output", "sql_query": output_query})

    sql_query = _build_sql_cte_statement(sql_query_list)

    return sql_query


def _sql_query(parameters_dict: dict) -> str:
    sql_query = (
        "{% if to_json is defined and to_json == true %}"
        'SELECT to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value FROM ('
        "{% endif %}"
        "{{ sql_statement }} "
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
        "{% if to_json is defined and to_json == true %}"
        ")"
        "{% endif %}"
    )

    sql_parameters = {
        "sql_statement": parameters_dict.get("sql_statement"),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "to_json": parameters_dict.get("to_json", False),
    }

    sql_template = Template(sql_query)
    return sql_template.render(sql_parameters)


def _sample_query_parameters(parameters_dict: dict) -> dict:
    sample_parameters = {
        "source": parameters_dict.get("source", None),
        "metadata_source": parameters_dict.get("metadata_source", None),
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
        "fill": parameters_dict.get("fill", False),
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
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "display_uom": parameters_dict.get("display_uom", False),
        "sort": parameters_dict.get("sort", True),
        "metadata_tagname_column": parameters_dict.get(
            "metadata_tagname_column", "TagName"
        ),
        "metadata_uom_column": parameters_dict.get("metadata_uom_column", "UoM"),
        "to_json_resample": parameters_dict.get("to_json", False),
    }
    return sample_parameters


def _sample_query(parameters_dict: dict) -> str:

    sample_parameters = _sample_query_parameters(parameters_dict)

    sql_query_list = []

    raw_query = _build_raw_query(
        sql_query_name="raw",
        timestamp_column=sample_parameters["timestamp_column"],
        tagname_column=sample_parameters["tagname_column"],
        status_column=sample_parameters["status_column"],
        value_column=sample_parameters["value_column"],
        start_date=sample_parameters["start_date"],
        end_date=sample_parameters["end_date"],
        time_interval_rate=sample_parameters["time_interval_rate"],
        time_interval_unit=sample_parameters["time_interval_unit"],
        agg_method=sample_parameters["agg_method"],
        time_zone=sample_parameters["time_zone"],
        source=sample_parameters["source"],
        business_unit=sample_parameters["business_unit"],
        asset=sample_parameters["asset"],
        data_security_level=sample_parameters["data_security_level"],
        data_type=sample_parameters["data_type"],
        tag_names=sample_parameters["tag_names"],
        include_status=sample_parameters["include_status"],
        case_insensitivity_tag_search=sample_parameters[
            "case_insensitivity_tag_search"
        ],
        sort=False,
    )

    sql_query_list.append({"query_name": "raw", "sql_query": raw_query})

    resample_query = _build_resample_query(
        sql_query_list=sql_query_list,
        sql_query_name="resample",
        timestamp_column=sample_parameters["timestamp_column"],
        tagname_column=sample_parameters["tagname_column"],
        value_column=sample_parameters["value_column"],
        tag_names=sample_parameters["tag_names"],
        start_date=sample_parameters["start_date"],
        end_date=sample_parameters["end_date"],
        time_zone=sample_parameters["time_zone"],
        time_interval_rate=sample_parameters["time_interval_rate"],
        time_interval_unit=sample_parameters["time_interval_unit"],
        agg_method=sample_parameters["agg_method"],
        case_insensitivity_tag_search=sample_parameters[
            "case_insensitivity_tag_search"
        ],
        fill=sample_parameters["fill"],
        sort=(
            sample_parameters["sort"] if sample_parameters["pivot"] == False else False
        ),
    )

    sql_query_list.append({"query_name": "resample", "sql_query": resample_query})

    if sample_parameters["pivot"] == True:
        pivot_query = _build_pivot_query(
            sql_query_list=sql_query_list,
            sql_query_name="pivot",
            tagname_column=sample_parameters["tagname_column"],
            timestamp_column=sample_parameters["timestamp_column"],
            value_column=sample_parameters["value_column"],
            tag_names=sample_parameters["tag_names"],
            is_case_insensitive_tag_search=sample_parameters[
                "case_insensitivity_tag_search"
            ],
            sort=sample_parameters["sort"],
        )

        sql_query_list.append({"query_name": "pivot", "sql_query": pivot_query})

    if sample_parameters["display_uom"] == True:
        uom_query = _build_uom_query(
            sql_query_list=sql_query_list,
            sql_query_name="uom",
            metadata_source=sample_parameters["metadata_source"],
            business_unit=sample_parameters["business_unit"],
            asset=sample_parameters["asset"],
            data_security_level=sample_parameters["data_security_level"],
            tagname_column=sample_parameters["tagname_column"],
            metadata_tagname_column=sample_parameters["metadata_tagname_column"],
            metadata_uom_column=sample_parameters["metadata_uom_column"],
        )

        sql_query_list.append({"query_name": "uom", "sql_query": uom_query})

    output_query = _build_output_query(
        sql_query_list=sql_query_list,
        to_json=sample_parameters["to_json_resample"],
        limit=sample_parameters["limit"],
        offset=sample_parameters["offset"],
    )

    sql_query_list.append({"query_name": "output", "sql_query": output_query})

    sql_query = _build_sql_cte_statement(sql_query_list)

    return sql_query


def _build_time_interval_array(
    sql_query_name,
    timestamp_column,
    start_date,
    end_date,
    time_zone,
    time_interval_rate,
    time_interval_unit,
):
    """Build time interval array for windowing operations."""
    time_interval_array_query = f"{sql_query_name} AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp('{start_date}'), '{time_zone}'), from_utc_timestamp(to_timestamp('{end_date}'), '{time_zone}'), INTERVAL '{time_interval_rate} {time_interval_unit}')) AS timestamp_array)"
    return time_interval_array_query


def _build_window_buckets(
    sql_query_list,
    sql_query_name,
    timestamp_column,
    time_interval_rate,
    time_interval_unit,
):
    """Build window buckets for time-based aggregations."""
    parent_sql_query_name = sql_query_list[-1]["query_name"]
    window_buckets_query = f"{sql_query_name} AS (SELECT timestamp_array AS window_start, timestampadd({time_interval_unit}, {time_interval_rate}, timestamp_array) AS window_end FROM {parent_sql_query_name})"
    return window_buckets_query


def _build_plot_aggregations(
    sql_query_list,
    sql_query_name,
    timestamp_column,
    tagname_column,
    value_column,
    status_column,
    range_join_seconds,
):
    """Build plot aggregations with OHLC (open, high, low, close) calculations."""
    parent_sql_query_name = sql_query_list[-1]["query_name"]
    raw_events_name = next(
        (
            query["query_name"]
            for query in sql_query_list
            if query["query_name"] == "raw_events"
        ),
        "raw_events",
    )

    plot_aggregations_query = f"{sql_query_name} AS (SELECT /*+ RANGE_JOIN(d, {range_join_seconds}) */ d.window_start, d.window_end, e.`{tagname_column}`, min(CASE WHEN `{status_column}` = 'Bad' THEN null ELSE struct(e.`{value_column}`, e.`{timestamp_column}`) END) OVER (PARTITION BY e.`{tagname_column}`, d.window_start ORDER BY e.`{timestamp_column}` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `min_{value_column}`, max(CASE WHEN `{status_column}` = 'Bad' THEN null ELSE struct(e.`{value_column}`, e.`{timestamp_column}`) END) OVER (PARTITION BY e.`{tagname_column}`, d.window_start ORDER BY e.`{timestamp_column}` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `max_{value_column}`, first(CASE WHEN `{status_column}` = 'Bad' THEN null ELSE struct(e.`{value_column}`, e.`{timestamp_column}`) END, True) OVER (PARTITION BY e.`{tagname_column}`, d.window_start ORDER BY e.`{timestamp_column}` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `first_{value_column}`, last(CASE WHEN `{status_column}` = 'Bad' THEN null ELSE struct(e.`{value_column}`, e.`{timestamp_column}`) END, True) OVER (PARTITION BY e.`{tagname_column}`, d.window_start ORDER BY e.`{timestamp_column}` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `last_{value_column}`, first(CASE WHEN `{status_column}` = 'Bad' THEN struct(e.`{value_column}`, e.`{timestamp_column}`) ELSE null END, True) OVER (PARTITION BY e.`{tagname_column}`, d.window_start ORDER BY e.`{timestamp_column}` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `excp_{value_column}` FROM {parent_sql_query_name} d INNER JOIN {raw_events_name} e ON d.window_start <= e.`{timestamp_column}` AND d.window_end > e.`{timestamp_column}`)"
    return plot_aggregations_query


def _build_plot_deduplication(
    sql_query_list,
    sql_query_name,
    timestamp_column,
    tagname_column,
    value_column,
):
    """Build deduplication step for plot aggregations."""
    parent_sql_query_name = sql_query_list[-1]["query_name"]
    deduplication_query = f"{sql_query_name} AS (SELECT window_start AS `{timestamp_column}`, `{tagname_column}`, `min_{value_column}` as `Min`, `max_{value_column}` as `Max`, `first_{value_column}` as `First`, `last_{value_column}` as `Last`, `excp_{value_column}` as `Exception` FROM {parent_sql_query_name} GROUP BY window_start, `{tagname_column}`, `min_{value_column}`, `max_{value_column}`, `first_{value_column}`, `last_{value_column}`, `excp_{value_column}`)"
    return deduplication_query


def _build_unpivot_projection(
    sql_query_list,
    sql_query_name,
    timestamp_column,
    tagname_column,
    value_column,
    sort=True,
):
    """Build unpivot projection to transform aggregated values into rows."""
    parent_sql_query_name = sql_query_list[-1]["query_name"]

    unpivot_query = f"{sql_query_name} AS (SELECT distinct Values.{timestamp_column}, `{tagname_column}`, Values.{value_column} FROM (SELECT * FROM {parent_sql_query_name} UNPIVOT (`Values` for `Aggregation` IN (`Min`, `Max`, `First`, `Last`, `Exception`)))"

    if sort:
        unpivot_query = " ".join(
            [unpivot_query, f"ORDER BY `{tagname_column}`, `{timestamp_column}`"]
        )

    return unpivot_query + ")"


def _plot_query_parameters(parameters_dict: dict) -> dict:
    """Extract and validate parameters for plot query."""
    plot_parameters = {
        "source": parameters_dict.get("source", None),
        "metadata_source": parameters_dict.get("metadata_source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "include_bad_data": True,
        "time_interval_rate": parameters_dict["time_interval_rate"],
        "time_interval_unit": parameters_dict["time_interval_unit"],
        "time_zone": parameters_dict["time_zone"],
        "pivot": parameters_dict.get("pivot", None),
        "display_uom": parameters_dict.get("display_uom", False),
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
        "range_join_seconds": parameters_dict["range_join_seconds"],
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "metadata_tagname_column": parameters_dict.get(
            "metadata_tagname_column", "TagName"
        ),
        "metadata_uom_column": parameters_dict.get("metadata_uom_column", "UoM"),
        "to_json": parameters_dict.get("to_json", False),
        "sort": parameters_dict.get("sort", True),
    }
    return plot_parameters


def _interpolation_query(parameters_dict: dict) -> str:

    parameters_dict["agg_method"] = None

    interpolate_parameters = _sample_query_parameters(parameters_dict)

    sql_query_list = []

    raw_query = _build_raw_query(
        sql_query_name="raw",
        timestamp_column=interpolate_parameters["timestamp_column"],
        tagname_column=interpolate_parameters["tagname_column"],
        status_column=interpolate_parameters["status_column"],
        value_column=interpolate_parameters["value_column"],
        start_date=interpolate_parameters["start_date"],
        end_date=interpolate_parameters["end_date"],
        time_interval_rate=interpolate_parameters["time_interval_rate"],
        time_interval_unit=interpolate_parameters["time_interval_unit"],
        agg_method=None,
        time_zone=interpolate_parameters["time_zone"],
        source=interpolate_parameters["source"],
        business_unit=interpolate_parameters["business_unit"],
        asset=interpolate_parameters["asset"],
        data_security_level=interpolate_parameters["data_security_level"],
        data_type=interpolate_parameters["data_type"],
        tag_names=interpolate_parameters["tag_names"],
        include_status=interpolate_parameters["include_status"],
        case_insensitivity_tag_search=interpolate_parameters[
            "case_insensitivity_tag_search"
        ],
        sort=False,
    )

    sql_query_list.append({"query_name": "raw", "sql_query": raw_query})

    # resample_query = _build_resample_query(
    #     sql_query_list=sql_query_list,
    #     sql_query_name="resample",
    #     timestamp_column=interpolate_parameters["timestamp_column"],
    #     tagname_column=interpolate_parameters["tagname_column"],
    #     value_column=interpolate_parameters["value_column"],
    #     tag_names=interpolate_parameters["tag_names"],
    #     start_date=interpolate_parameters["start_date"],
    #     end_date=interpolate_parameters["end_date"],
    #     time_zone=interpolate_parameters["time_zone"],
    #     time_interval_rate=interpolate_parameters["time_interval_rate"],
    #     time_interval_unit=interpolate_parameters["time_interval_unit"],
    #     agg_method=interpolate_parameters["agg_method"],
    #     case_insensitivity_tag_search=interpolate_parameters[
    #         "case_insensitivity_tag_search"
    #     ],
    #     fill=True,
    #     sort=False,
    # )

    # sql_query_list.append({"query_name": "resample", "sql_query": resample_query})
    fill_intervals_query = _build_fill_intervals_query(
        sql_query_list=sql_query_list,
        sql_query_name="fill_intervals",
        timestamp_column=interpolate_parameters["timestamp_column"],
        tagname_column=interpolate_parameters["tagname_column"],
        value_column=interpolate_parameters["value_column"],
        tag_names=interpolate_parameters["tag_names"],
        start_date=interpolate_parameters["start_date"],
        end_date=interpolate_parameters["end_date"],
        time_zone=interpolate_parameters["time_zone"],
        time_interval_rate=interpolate_parameters["time_interval_rate"],
        time_interval_unit=interpolate_parameters["time_interval_unit"],
        case_insensitivity_tag_search=interpolate_parameters[
            "case_insensitivity_tag_search"
        ],
    )

    sql_query_list.append(
        {"query_name": "fill_intervals", "sql_query": fill_intervals_query}
    )

    interpolate_query = _build_interpolate_query(
        sql_query_list=sql_query_list,
        sql_query_name="interpolate",
        timestamp_column=interpolate_parameters["timestamp_column"],
        tagname_column=interpolate_parameters["tagname_column"],
        value_column=interpolate_parameters["value_column"],
        sort=(
            interpolate_parameters["sort"]
            if interpolate_parameters["pivot"] == False
            else False
        ),
    )

    sql_query_list.append({"query_name": "interpolate", "sql_query": interpolate_query})

    if interpolate_parameters["pivot"] == True:
        pivot_query = _build_pivot_query(
            sql_query_list=sql_query_list,
            sql_query_name="pivot",
            tagname_column=interpolate_parameters["tagname_column"],
            timestamp_column=interpolate_parameters["timestamp_column"],
            value_column=interpolate_parameters["value_column"],
            tag_names=interpolate_parameters["tag_names"],
            is_case_insensitive_tag_search=interpolate_parameters[
                "case_insensitivity_tag_search"
            ],
            sort=interpolate_parameters["sort"],
        )

        sql_query_list.append({"query_name": "pivot", "sql_query": pivot_query})

    if interpolate_parameters["display_uom"] == True:
        uom_query = _build_uom_query(
            sql_query_list=sql_query_list,
            sql_query_name="uom",
            metadata_source=interpolate_parameters["metadata_source"],
            business_unit=interpolate_parameters["business_unit"],
            asset=interpolate_parameters["asset"],
            data_security_level=interpolate_parameters["data_security_level"],
            tagname_column=interpolate_parameters["tagname_column"],
            metadata_tagname_column=interpolate_parameters["metadata_tagname_column"],
            metadata_uom_column=interpolate_parameters["metadata_uom_column"],
        )

        sql_query_list.append({"query_name": "uom", "sql_query": uom_query})

    output_query = _build_output_query(
        sql_query_list=sql_query_list,
        to_json=interpolate_parameters["to_json_resample"],
        limit=interpolate_parameters["limit"],
        offset=interpolate_parameters["offset"],
    )

    sql_query_list.append({"query_name": "output", "sql_query": output_query})

    sql_query = _build_sql_cte_statement(sql_query_list)

    return sql_query


def _plot_query(parameters_dict: dict) -> str:

    plot_parameters = _plot_query_parameters(parameters_dict)

    sql_query_list = []

    # Build raw events query
    raw_query = _build_raw_query(
        sql_query_name="raw_events",
        timestamp_column=plot_parameters["timestamp_column"],
        tagname_column=plot_parameters["tagname_column"],
        status_column=plot_parameters["status_column"],
        value_column=plot_parameters["value_column"],
        start_date=plot_parameters["start_date"],
        end_date=plot_parameters["end_date"],
        time_zone=plot_parameters["time_zone"],
        deduplicate=True,
        source=plot_parameters["source"],
        business_unit=plot_parameters["business_unit"],
        asset=plot_parameters["asset"],
        data_security_level=plot_parameters["data_security_level"],
        data_type=plot_parameters["data_type"],
        tag_names=plot_parameters["tag_names"],
        include_status=plot_parameters["include_status"],
        include_bad_data=plot_parameters["include_bad_data"],
        case_insensitivity_tag_search=plot_parameters["case_insensitivity_tag_search"],
        sort=False,
    )

    sql_query_list.append({"query_name": "raw_events", "sql_query": raw_query})

    # Build time interval array
    time_interval_query = _build_time_interval_array(
        sql_query_name="date_array",
        timestamp_column=plot_parameters["timestamp_column"],
        start_date=plot_parameters["start_date"],
        end_date=plot_parameters["end_date"],
        time_zone=plot_parameters["time_zone"],
        time_interval_rate=plot_parameters["time_interval_rate"],
        time_interval_unit=plot_parameters["time_interval_unit"],
    )

    sql_query_list.append(
        {"query_name": "date_array", "sql_query": time_interval_query}
    )

    # Build window buckets
    window_buckets_query = _build_window_buckets(
        sql_query_list=sql_query_list,
        sql_query_name="window_buckets",
        timestamp_column=plot_parameters["timestamp_column"],
        time_interval_rate=plot_parameters["time_interval_rate"],
        time_interval_unit=plot_parameters["time_interval_unit"],
    )

    sql_query_list.append(
        {"query_name": "window_buckets", "sql_query": window_buckets_query}
    )

    # Build plot aggregations
    plot_aggregations_query = _build_plot_aggregations(
        sql_query_list=sql_query_list,
        sql_query_name="plot",
        timestamp_column=plot_parameters["timestamp_column"],
        tagname_column=plot_parameters["tagname_column"],
        value_column=plot_parameters["value_column"],
        status_column=plot_parameters["status_column"],
        range_join_seconds=plot_parameters["range_join_seconds"],
    )

    sql_query_list.append({"query_name": "plot", "sql_query": plot_aggregations_query})

    # Build deduplication
    deduplication_query = _build_plot_deduplication(
        sql_query_list=sql_query_list,
        sql_query_name="deduplicate",
        timestamp_column=plot_parameters["timestamp_column"],
        tagname_column=plot_parameters["tagname_column"],
        value_column=plot_parameters["value_column"],
    )

    sql_query_list.append(
        {"query_name": "deduplicate", "sql_query": deduplication_query}
    )

    # Build unpivot projection
    unpivot_query = _build_unpivot_projection(
        sql_query_list=sql_query_list,
        sql_query_name="project",
        timestamp_column=plot_parameters["timestamp_column"],
        tagname_column=plot_parameters["tagname_column"],
        value_column=plot_parameters["value_column"],
        sort=(plot_parameters["sort"] if plot_parameters["pivot"] == False else False),
    )

    sql_query_list.append({"query_name": "project", "sql_query": unpivot_query})

    # Add pivot if requested
    if plot_parameters["pivot"] == True:
        pivot_query = _build_pivot_query(
            sql_query_list=sql_query_list,
            sql_query_name="pivot",
            tagname_column=plot_parameters["tagname_column"],
            timestamp_column=plot_parameters["timestamp_column"],
            value_column=plot_parameters["value_column"],
            tag_names=plot_parameters["tag_names"],
            is_case_insensitive_tag_search=plot_parameters[
                "case_insensitivity_tag_search"
            ],
            sort=True,
        )

        sql_query_list.append({"query_name": "pivot", "sql_query": pivot_query})

    # Add UOM if requested
    if plot_parameters["display_uom"] == True:
        uom_query = _build_uom_query(
            sql_query_list=sql_query_list,
            sql_query_name="uom",
            metadata_source=plot_parameters["metadata_source"],
            business_unit=plot_parameters["business_unit"],
            asset=plot_parameters["asset"],
            data_security_level=plot_parameters["data_security_level"],
            tagname_column=plot_parameters["tagname_column"],
            metadata_tagname_column=plot_parameters["metadata_tagname_column"],
            metadata_uom_column=plot_parameters["metadata_uom_column"],
        )

        sql_query_list.append({"query_name": "uom", "sql_query": uom_query})

    # Build output query
    output_query = _build_output_query(
        sql_query_list=sql_query_list,
        to_json=plot_parameters["to_json"],
        limit=plot_parameters["limit"],
        offset=plot_parameters["offset"],
    )

    sql_query_list.append({"query_name": "output", "sql_query": output_query})

    # Build final SQL
    sql_query = _build_sql_cte_statement(sql_query_list)

    return sql_query


def _interpolation_at_time(parameters_dict: dict) -> str:
    timestamps_deduplicated = list(
        dict.fromkeys(parameters_dict["timestamps"])
    )  # remove potential duplicates in tags
    parameters_dict["timestamps"] = timestamps_deduplicated.copy()
    parameters_dict["min_timestamp"] = min(timestamps_deduplicated)
    parameters_dict["max_timestamp"] = max(timestamps_deduplicated)

    interpolate_at_time_query = (
        'WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(date_trunc("millisecond",`{{ timestamp_column }}`), "{{ time_zone }}") AS `{{ timestamp_column }}`, `{{ tagname_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% else %} \'Good\' AS `Status`, {% endif %} `{{ value_column }}` FROM '
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "WHERE to_date(`{{ timestamp_column }}`) BETWEEN "
        "{% if timestamps is defined %} "
        'date_sub(to_date(to_timestamp("{{ min_timestamp }}")), {{ window_length }}) AND date_add(to_date(to_timestamp("{{ max_timestamp }}")), {{ window_length}}) '
        "{% endif %} "
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        "AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}')"
        "{% else %}"
        "AND `{{ tagname_column }}` IN ('{{ tag_names | join('\\', \\'') }}')"
        "{% endif %} "
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND `{{ status_column }}` <> 'Bad' {% endif %}) "
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        ", date_array AS (SELECT DISTINCT explode(array("
        "{% else %}"
        ", date_array AS (SELECT explode(array( "
        "{% endif %} "
        "{% for timestamp in timestamps -%} "
        'from_utc_timestamp(to_timestamp("{{timestamp}}"), "{{time_zone}}") '
        "{% if not loop.last %} , {% endif %} {% endfor %} )) AS `{{ timestamp_column }}`, "
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        "explode(array(`{{ tagname_column }}`)) AS `{{ tagname_column }}` FROM raw_events)"
        "{% else %}"
        "explode(array('{{ tag_names | join('\\', \\'') }}')) AS `{{ tagname_column }}`) "
        "{% endif %} "
        ", interpolation_events AS (SELECT coalesce(a.`{{ tagname_column }}`, b.`{{ tagname_column }}`) AS `{{ tagname_column }}`, coalesce(a.`{{ timestamp_column }}`, b.`{{ timestamp_column }}`) AS `{{ timestamp_column }}`, a.`{{ timestamp_column }}` AS `Requested_{{ timestamp_column }}`, b.`{{ timestamp_column }}` AS `Found_{{ timestamp_column }}`, b.`{{ status_column }}`, b.`{{ value_column }}` FROM date_array a FULL OUTER JOIN  raw_events b ON a.`{{ timestamp_column }}` = b.`{{ timestamp_column }}` AND a.`{{ tagname_column }}` = b.`{{ tagname_column }}`) "
        ", interpolation_calculations AS (SELECT *, lag(`Found_{{ timestamp_column }}`) IGNORE NULLS OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Previous_{{ timestamp_column }}`, lag(`{{ value_column }}`) IGNORE NULLS OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Previous_{{ value_column }}`, lead(`Found_{{ timestamp_column }}`) IGNORE NULLS OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Next_{{ timestamp_column }}`, lead(`{{ value_column }}`) IGNORE NULLS OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Next_{{ value_column }}`, "
        "CASE WHEN `Requested_{{ timestamp_column }}` = `Found_{{ timestamp_column }}` THEN `{{ value_column }}` WHEN `Next_{{ timestamp_column }}` IS NULL THEN `Previous_{{ value_column }}` WHEN `Previous_{{ timestamp_column }}` IS NULL AND `Next_{{ timestamp_column }}` IS NULL THEN NULL "
        "ELSE `Previous_{{ value_column }}` + ((`Next_{{ value_column }}` - `Previous_{{ value_column }}`) * ((unix_timestamp(`{{ timestamp_column }}`) - unix_timestamp(`Previous_{{ timestamp_column }}`)) / (unix_timestamp(`Next_{{ timestamp_column }}`) - unix_timestamp(`Previous_{{ timestamp_column }}`)))) END AS `Interpolated_{{ value_column }}` FROM interpolation_events) "
        ",project AS (SELECT `{{ tagname_column }}`, `{{ timestamp_column }}`, `Interpolated_{{ value_column }}` AS `{{ value_column }}` FROM interpolation_calculations WHERE `{{ timestamp_column }}` IN ( "
        "{% for timestamp in timestamps -%} "
        'from_utc_timestamp(to_timestamp("{{timestamp}}"), "{{time_zone}}") '
        "{% if not loop.last %} , {% endif %} {% endfor %}) "
        ") "
        "{% if pivot is defined and pivot == true %}"
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
        "{% for i in range(tag_names | length) %}"
        "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
        "{% endfor %}"
        "{% else %}"
        ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, `{{ tagname_column }}` AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
        "{% for i in range(tag_names | length) %}"
        "'{{ tag_names[i] }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
        "{% endfor %}"
        "{% endif %}"
        '))) SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM pivot ORDER BY `{{ timestamp_column }}` '
        "{% else %}"
        "{% if display_uom is defined and display_uom == true %}"
        'SELECT {% if to_json is defined and to_json == true %}to_json(struct(p.`EventTime`, p.`TagName`, p.`Value`, m.`UoM`), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}p.`EventTime`, p.`TagName`, p.`Value`, m.`UoM`{% endif %} FROM project p '
        "LEFT OUTER JOIN "
        "{% if metadata_source is defined and metadata_source is not none %}"
        "{{ metadata_source|lower }} m ON p.`{{ tagname_column }}` = m.`{{ metadata_tagname_column }}` ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` m ON p.`{{ tagname_column }}` = m.`{{ tagname_column }}` ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% endif %}"
        "{% else%}"
        'SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` '
        "{% endif %}"
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
        "metadata_source": parameters_dict.get("metadata_source", None),
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
        "display_uom": parameters_dict.get("display_uom", False),
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
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "metadata_tagname_column": parameters_dict.get(
            "metadata_tagname_column", "TagName"
        ),
        "metadata_uom_column": parameters_dict.get("metadata_uom_column", "UoM"),
        "to_json": parameters_dict.get("to_json", False),
    }
    sql_template = Template(interpolate_at_time_query)
    return sql_template.render(interpolation_at_time_parameters)


def _metadata_query(parameters_dict: dict) -> str:
    metadata_query = (
        'SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM '
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` "
        "{% endif %}"
        "{% if tag_names is defined and tag_names|length > 0 %}"
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        " WHERE UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% else %}"
        " WHERE `{{ tagname_column }}` IN ('{{ tag_names | join('\\', \\'') }}') "
        "{% endif %}"
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
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "to_json": parameters_dict.get("to_json", False),
    }

    sql_template = Template(metadata_query)
    return sql_template.render(metadata_parameters)


def _latest_query(parameters_dict: dict) -> str:
    latest_query = (
        "WITH latest AS (SELECT * FROM "
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_latest` "
        "{% endif %}"
        "{% if tag_names is defined and tag_names|length > 0 %}"
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        " WHERE UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% else %}"
        " WHERE `{{ tagname_column }}` IN ('{{ tag_names | join('\\', \\'') }}') "
        "{% endif %}"
        "{% endif %}"
        "ORDER BY `{{ tagname_column }}` ) "
        "{% if display_uom is defined and display_uom == true %}"
        'SELECT {% if to_json is defined and to_json == true %}to_json(struct(l.*, m.`UoM), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}l.*, m.`UoM`{% endif %} FROM latest l '
        "LEFT OUTER JOIN "
        "{% if metadata_source is defined and metadata_source is not none %}"
        "{{ metadata_source|lower }} m ON l.`{{ tagname_column }}` = m.`{{ metadata_tagname_column }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` m ON l.`{{ tagname_column }}` = m.`{{ tagname_column }}` "
        "{% endif %}"
        "{% else %}"
        'SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM latest '
        "{% endif %}"
        "{% if limit is defined and limit is not none %}"
        "LIMIT {{ limit }} "
        "{% endif %}"
        "{% if offset is defined and offset is not none %}"
        "OFFSET {{ offset }} "
        "{% endif %}"
    )

    latest_parameters = {
        "source": parameters_dict.get("source", None),
        "metadata_source": parameters_dict.get("metadata_source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "display_uom": parameters_dict.get("display_uom", False),
        "limit": parameters_dict.get("limit", None),
        "offset": parameters_dict.get("offset", None),
        "tagname_column": parameters_dict.get("tagname_column", "TagName"),
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "metadata_tagname_column": parameters_dict.get(
            "metadata_tagname_column", "TagName"
        ),
        "metadata_uom_column": parameters_dict.get("metadata_uom_column", "UoM"),
        "to_json": parameters_dict.get("to_json", False),
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
        'WITH raw_events AS (SELECT DISTINCT `{{ tagname_column }}`, from_utc_timestamp(date_trunc("millisecond",`{{ timestamp_column }}`), "{{ time_zone }}") AS `{{ timestamp_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% else %} \'Good\' AS `Status`, {% endif %} `{{ value_column }}` FROM '
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        "WHERE to_date(`{{ timestamp_column }}`) BETWEEN date_sub(to_date(to_timestamp(\"{{ start_date }}\")), {{ window_length }}) AND date_add(to_date(to_timestamp(\"{{ end_date }}\")), {{ window_length }}) AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}')  "
        "{% else %}"
        "WHERE to_date(`{{ timestamp_column }}`) BETWEEN date_sub(to_date(to_timestamp(\"{{ start_date }}\")), {{ window_length }}) AND date_add(to_date(to_timestamp(\"{{ end_date }}\")), {{ window_length }}) AND `{{ tagname_column }}` IN ('{{ tag_names | join('\\', \\'') }}') "
        "{% endif %}"
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND `{{ status_column }}` <> 'Bad' {% endif %}) "
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        ',date_array AS (SELECT DISTINCT explode(sequence(from_utc_timestamp(to_timestamp("{{ start_date }}"), "{{ time_zone }}"), from_utc_timestamp(to_timestamp("{{ end_date }}"), "{{ time_zone }}"), INTERVAL \'{{ time_interval_rate + \' \' + time_interval_unit }}\')) AS `{{ timestamp_column }}`, explode(array(`{{ tagname_column }}`)) AS `{{ tagname_column }}` FROM raw_events) '
        "{% else %}"
        ",date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp(\"{{ start_date }}\"), \"{{ time_zone }}\"), from_utc_timestamp(to_timestamp(\"{{ end_date }}\"), \"{{ time_zone }}\"), INTERVAL '{{ time_interval_rate + ' ' + time_interval_unit }}')) AS `{{ timestamp_column }}`, explode(array('{{ tag_names | join('\\', \\'') }}')) AS `{{ tagname_column }}`) "
        "{% endif %}"
        ",boundary_events AS (SELECT coalesce(a.`{{ tagname_column }}`, b.`{{ tagname_column }}`) AS `{{ tagname_column }}`, coalesce(a.`{{ timestamp_column }}`, b.`{{ timestamp_column }}`) AS `{{ timestamp_column }}`, b.`{{ status_column }}`, b.`{{ value_column }}` FROM date_array a FULL OUTER JOIN raw_events b ON a.`{{ timestamp_column }}` = b.`{{ timestamp_column }}` AND a.`{{ tagname_column }}` = b.`{{ tagname_column }}`) "
        ",window_buckets AS (SELECT `{{ timestamp_column }}` AS window_start, LEAD(`{{ timestamp_column }}`) OVER (ORDER BY `{{ timestamp_column }}`) AS window_end FROM (SELECT distinct `{{ timestamp_column }}` FROM date_array) ) "
        ",window_events AS (SELECT /*+ RANGE_JOIN(b, {{ range_join_seconds }} ) */ b.`{{ tagname_column }}`, b.`{{ timestamp_column }}`, a.window_start AS `Window{{ timestamp_column }}`, b.`{{ status_column }}`, b.`{{ value_column }}` FROM boundary_events b LEFT OUTER JOIN window_buckets a ON a.window_start <= b.`{{ timestamp_column }}` AND a.window_end > b.`{{ timestamp_column }}`) "
        ',fill_status AS (SELECT *, last_value(`{{ status_column }}`, true) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_{{ status_column }}`, CASE WHEN `Fill_{{ status_column }}` <> "Bad" THEN `{{ value_column }}` ELSE null END AS `Good_{{ value_column }}` FROM window_events) '
        ",fill_value AS (SELECT *, last_value(`Good_{{ value_column }}`, true) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_{{ value_column }}` FROM fill_status) "
        '{% if step is defined and step == "metadata" %} '
        ",fill_step AS (SELECT f.*, IFNULL(m.Step, false) AS Step FROM fill_value f "
        "LEFT JOIN "
        "{% if metadata_source is defined and metadata_source is not none %}"
        "{{ metadata_source|lower }} m ON f.`{{ tagname_column }}` = m.`{{ metadata_tagname_column }}`) "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` m ON f.`{{ tagname_column }}` = m.`{{ tagname_column }}`) "
        "{% endif %}"
        # "LEFT JOIN "
        # "{% if source_metadata is defined and source_metadata is not none %}"
        # "`{{ source_metadata|lower }}` "
        # "{% else %}"
        # "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` "
        # "{% endif %}"
        # "m ON f.`{{ tagname_column }}` = m.`{{ tagname_column }}`) "
        "{% else %}"
        ",fill_step AS (SELECT *, {{ step }} AS Step FROM fill_value) "
        "{% endif %}"
        ",interpolate AS (SELECT *, CASE WHEN `Step` = false AND `{{ status_column }}` IS NULL AND `{{ value_column }}` IS NULL THEN lag(`{{ timestamp_column }}`) OVER ( PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ) ELSE NULL END AS `Previous_{{ timestamp_column }}`, CASE WHEN `Step` = false AND `{{ status_column }}` IS NULL AND `{{ value_column }}` IS NULL THEN lag(`Fill_{{ value_column }}`) OVER ( PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ) ELSE NULL END AS `Previous_Fill_{{ value_column }}`, "
        "lead(`{{ timestamp_column }}`) OVER ( PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ) AS `Next_{{ timestamp_column }}`, CASE WHEN `Step` = false AND `{{ status_column }}` IS NULL AND `{{ value_column }}` IS NULL THEN lead(`Fill_{{ value_column }}`) OVER ( PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}` ) ELSE NULL END AS `Next_Fill_{{ value_column }}`, CASE WHEN `Step` = false AND `{{ status_column }}` IS NULL AND `{{ value_column }}` IS NULL THEN `Previous_Fill_{{ value_column }}` + ( (`Next_Fill_{{ value_column }}` - `Previous_Fill_{{ value_column }}`) * ( ( unix_timestamp(`{{ timestamp_column }}`) - unix_timestamp(`Previous_{{ timestamp_column }}`) ) / ( unix_timestamp(`Next_{{ timestamp_column }}`) - unix_timestamp(`Previous_{{ timestamp_column }}`) ) ) ) ELSE NULL END AS `Interpolated_{{ value_column }}`, coalesce(`Interpolated_{{ value_column }}`, `Fill_{{ value_column }}`) as `Event_{{ value_column }}` FROM fill_step )"
        ",twa_calculations AS (SELECT `{{ tagname_column }}`, `{{ timestamp_column }}`, `Window{{ timestamp_column }}`, `Step`, `{{ status_column }}`, `{{ value_column }}`, `Previous_{{ timestamp_column }}`, `Previous_Fill_{{ value_column }}`, `Next_{{ timestamp_column }}`, `Next_Fill_{{ value_column }}`, `Interpolated_{{ value_column }}`, `Fill_{{ status_column }}`, `Fill_{{ value_column }}`, `Event_{{ value_column }}`, lead(`Fill_{{ status_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) AS `Next_{{ status_column }}` "
        ', CASE WHEN `Next_{{ status_column }}` <> "Bad" OR (`Fill_{{ status_column }}` <> "Bad" AND `Next_{{ status_column }}` = "Bad") THEN lead(`Event_{{ value_column }}`) OVER (PARTITION BY `{{ tagname_column }}` ORDER BY `{{ timestamp_column }}`) ELSE `{{ value_column }}` END AS `Next_{{ value_column }}_For_{{ status_column }}` '
        ', CASE WHEN `Fill_{{ status_column }}` <> "Bad" THEN `Next_{{ value_column }}_For_{{ status_column }}` ELSE 0 END AS `Next_{{ value_column }}` '
        ', CASE WHEN `Fill_{{ status_column }}` <> "Bad" AND `Next_{{ status_column }}` <> "Bad" THEN ((cast(`Next_{{ timestamp_column }}` AS double) - cast(`{{ timestamp_column }}` AS double)) / 60) WHEN `Fill_{{ status_column }}` <> "Bad" AND `Next_{{ status_column }}` = "Bad" THEN ((cast(`Next_{{ timestamp_column }}` AS integer) - cast(`{{ timestamp_column }}` AS double)) / 60) ELSE 0 END AS good_minutes '
        ", CASE WHEN Step == false THEN ((`Event_{{ value_column }}` + `Next_{{ value_column }}`) * 0.5) * good_minutes ELSE (`Event_{{ value_column }}` * good_minutes) END AS twa_value FROM interpolate) "
        ",twa AS (SELECT `{{ tagname_column }}`, `Window{{ timestamp_column }}` AS `{{ timestamp_column }}`, sum(twa_value) / sum(good_minutes) AS `{{ value_column }}` from twa_calculations GROUP BY `{{ tagname_column }}`, `Window{{ timestamp_column }}`) "
        ',project AS (SELECT * FROM twa WHERE `{{ timestamp_column }}` BETWEEN to_timestamp("{{ start_datetime }}") AND to_timestamp("{{ end_datetime }}")) '
        "{% if pivot is defined and pivot == true %}"
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
        "{% for i in range(tag_names | length) %}"
        "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
        "{% endfor %}"
        "{% else %}"
        ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, `{{ tagname_column }}` AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
        "{% for i in range(tag_names | length) %}"
        "'{{ tag_names[i] }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
        "{% endfor %}"
        "{% endif %}"
        '))) SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM pivot ORDER BY `{{ timestamp_column }}` '
        "{% else %}"
        "{% if display_uom is defined and display_uom == true %}"
        'SELECT {% if to_json is defined and to_json == true %}to_json(struct(p.`EventTime`, p.`TagName`, p.`Value`, m.`UoM`), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}p.`EventTime`, p.`TagName`, p.`Value`, m.`UoM`{% endif %} FROM project p '
        "LEFT OUTER JOIN "
        "{% if metadata_source is defined and metadata_source is not none %}"
        "`{{ metadata_source|lower }}` m ON p.`{{ tagname_column }}` = m.`{{ metadata_tagname_column }}` ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` m ON p.`{{ tagname_column }}` = m.`{{ tagname_column }}` ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
        "{% endif %}"
        "{% else%}"
        'SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` '
        "{% endif %}"
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
        "metadata_source": parameters_dict.get("metadata_source", None),
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
        "display_uom": parameters_dict.get("display_uom", False),
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
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "metadata_tagname_column": parameters_dict.get(
            "metadata_tagname_column", "TagName"
        ),
        "metadata_uom_column": parameters_dict.get("metadata_uom_column", "UoM"),
        "to_json": parameters_dict.get("to_json", False),
    }

    sql_template = Template(time_weighted_average_query)
    return sql_template.render(time_weighted_average_parameters)


def _circular_stats_query(parameters_dict: dict) -> str:
    circular_base_query = (
        'WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(date_trunc("millisecond",`{{ timestamp_column }}`), "{{ time_zone }}") AS `{{ timestamp_column }}`, `{{ tagname_column }}`, {% if include_status is defined and include_status == true %} `{{ status_column }}`, {% else %} \'Good\' AS `Status`, {% endif %} `{{ value_column }}` FROM '
        "{% if source is defined and source is not none %}"
        "`{{ source|lower }}` "
        "{% else %}"
        "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_events_{{ data_type|lower }}` "
        "{% endif %}"
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        "WHERE `{{ timestamp_column }}` BETWEEN TO_TIMESTAMP(\"{{ start_date }}\") AND TO_TIMESTAMP(\"{{ end_date }}\") AND UPPER(`{{ tagname_column }}`) IN ('{{ tag_names | join('\\', \\'') | upper }}') "
        "{% else %}"
        "WHERE `{{ timestamp_column }}` BETWEEN TO_TIMESTAMP(\"{{ start_date }}\") AND TO_TIMESTAMP(\"{{ end_date }}\") AND `{{ tagname_column }}` IN ('{{ tag_names | join('\\', \\'') }}') "
        "{% endif %}"
        "{% if include_status is defined and include_status == true and include_bad_data is defined and include_bad_data == false %} AND `{{ status_column }}` <> 'Bad' {% endif %}) "
        "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
        ',date_array AS (SELECT DISTINCT EXPLODE(SEQUENCE(FROM_UTC_TIMESTAMP(TO_TIMESTAMP("{{ start_date }}"), "{{ time_zone }}"), FROM_UTC_TIMESTAMP(TO_TIMESTAMP("{{ end_date }}"), "{{ time_zone }}"), INTERVAL \'{{ time_interval_rate + \' \' + time_interval_unit }}\')) AS `{{ timestamp_column }}`, EXPLODE(ARRAY(`{{ tagname_column }}`)) AS `{{ tagname_column }}` FROM raw_events)  '
        "{% else %}"
        ",date_array AS (SELECT EXPLODE(SEQUENCE(FROM_UTC_TIMESTAMP(TO_TIMESTAMP(\"{{ start_date }}\"), \"{{ time_zone }}\"), FROM_UTC_TIMESTAMP(TO_TIMESTAMP(\"{{ end_date }}\"), \"{{ time_zone }}\"), INTERVAL '{{ time_interval_rate + ' ' + time_interval_unit }}')) AS `{{ timestamp_column }}`, EXPLODE(ARRAY('{{ tag_names | join('\\', \\'') }}')) AS `{{ tagname_column }}`) "
        "{% endif %}"
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
            "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
            ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
            "{% for i in range(tag_names | length) %}"
            "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
            "{% endfor %}"
            "{% else %}"
            ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, `{{ tagname_column }}` AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
            "{% for i in range(tag_names | length) %}"
            "'{{ tag_names[i] }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
            "{% endfor %}"
            "{% endif %}"
            '))) SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM pivot ORDER BY `{{ timestamp_column }}` '
            "{% else %}"
            "{% if display_uom is defined and display_uom == true %}"
            'SELECT {% if to_json is defined and to_json == true %}to_json(struct(p.*, m.`UoM`), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}p.*, m.`UoM`{% endif %} FROM project p '
            "LEFT OUTER JOIN "
            "{% if metadata_source is defined and metadata_source is not none %}"
            "{{ metadata_source|lower }} m ON p.`{{ tagname_column }}` = m.`{{ metadata_tagname_column }}` ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
            "{% else %}"
            "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` m ON p.`{{ tagname_column }}` = m.`{{ tagname_column }}` ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
            "{% endif %}"
            "{% else%}"
            'SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` '
            "{% endif %}"
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
            "{% if case_insensitivity_tag_search is defined and case_insensitivity_tag_search == true %}"
            ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, UPPER(`{{ tagname_column }}`) AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
            "{% for i in range(tag_names | length) %}"
            "'{{ tag_names[i] | upper }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
            "{% endfor %}"
            "{% else %}"
            ",pivot AS (SELECT * FROM (SELECT `{{ timestamp_column }}`, `{{ value_column }}`, `{{ tagname_column }}` AS `{{ tagname_column }}` FROM project) PIVOT (FIRST(`{{ value_column }}`) FOR `{{ tagname_column }}` IN ("
            "{% for i in range(tag_names | length) %}"
            "'{{ tag_names[i] }}' AS `{{ tag_names[i] }}`{% if not loop.last %}, {% endif %}"
            "{% endfor %}"
            "{% endif %}"
            '))) SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM pivot ORDER BY `{{ timestamp_column }}` '
            "{% else %}"
            "{% if display_uom is defined and display_uom == true %}"
            'SELECT {% if to_json is defined and to_json == true %}to_json(struct(p.*, m.`UoM`), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}p.*, m.`UoM`{% endif %} FROM project p '
            "LEFT OUTER JOIN "
            "{% if metadata_source is defined and metadata_source is not none %}"
            "{{ metadata_source|lower }} m ON p.`{{ tagname_column }}` = m.`{{ metadata_tagname_column }}` ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
            "{% else %}"
            "`{{ business_unit|lower }}`.`sensors`.`{{ asset|lower }}_{{ data_security_level|lower }}_metadata` m ON p.`{{ tagname_column }}` = m.`{{ tagname_column }}` ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` "
            "{% endif %}"
            "{% else%}"
            'SELECT {% if to_json is defined and to_json == true %}to_json(struct(*), map("timestampFormat", "yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSSSSXXX")) as Value{% else %}*{% endif %} FROM project ORDER BY `{{ tagname_column }}`, `{{ timestamp_column }}` '
            "{% endif %}"
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
        "metadata_source": parameters_dict.get("metadata_source", None),
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
        "display_uom": parameters_dict.get("display_uom", False),
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
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "metadata_tagname_column": parameters_dict.get(
            "metadata_tagname_column", "TagName"
        ),
        "metadata_uom_column": parameters_dict.get("metadata_uom_column", "UoM"),
        "to_json": parameters_dict.get("to_json", False),
    }

    sql_template = Template(circular_stats_query)
    return sql_template.render(circular_stats_parameters)


def _summary_query(parameters_dict: dict) -> str:
    sql_query_list = []

    summary_parameters = {
        "source": parameters_dict.get("source", None),
        "metadata_source": parameters_dict.get("metadata_source", None),
        "business_unit": parameters_dict.get("business_unit"),
        "region": parameters_dict.get("region"),
        "asset": parameters_dict.get("asset"),
        "data_security_level": parameters_dict.get("data_security_level"),
        "data_type": parameters_dict.get("data_type"),
        "start_date": parameters_dict["start_date"],
        "end_date": parameters_dict["end_date"],
        "tag_names": list(dict.fromkeys(parameters_dict["tag_names"])),
        "include_bad_data": parameters_dict["include_bad_data"],
        "display_uom": parameters_dict.get("display_uom", False),
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
        "case_insensitivity_tag_search": parameters_dict.get(
            "case_insensitivity_tag_search", False
        ),
        "metadata_tagname_column": parameters_dict.get(
            "metadata_tagname_column", "TagName"
        ),
        "metadata_uom_column": parameters_dict.get("metadata_uom_column", "UoM"),
        "to_json": parameters_dict.get("to_json", False),
    }

    summary_query = _build_summary_query(
        sql_query_name="summary",
        timestamp_column=summary_parameters["timestamp_column"],
        tagname_column=summary_parameters["tagname_column"],
        status_column=summary_parameters["status_column"],
        value_column=summary_parameters["value_column"],
        start_date=summary_parameters["start_date"],
        end_date=summary_parameters["end_date"],
        source=summary_parameters["source"],
        business_unit=summary_parameters["business_unit"],
        asset=summary_parameters["asset"],
        data_security_level=summary_parameters["data_security_level"],
        data_type=summary_parameters["data_type"],
        tag_names=summary_parameters["tag_names"],
        include_status=summary_parameters["include_status"],
        include_bad_data=summary_parameters["include_bad_data"],
        case_insensitivity_tag_search=summary_parameters[
            "case_insensitivity_tag_search"
        ],
    )

    sql_query_list.append({"query_name": "summary", "sql_query": summary_query})

    if summary_parameters["display_uom"] == True:
        uom_query = _build_uom_query(
            sql_query_list=sql_query_list,
            sql_query_name="uom",
            metadata_source=summary_parameters["metadata_source"],
            business_unit=summary_parameters["business_unit"],
            asset=summary_parameters["asset"],
            data_security_level=summary_parameters["data_security_level"],
            tagname_column=summary_parameters["tagname_column"],
            metadata_tagname_column=summary_parameters["metadata_tagname_column"],
            metadata_uom_column=summary_parameters["metadata_uom_column"],
        )
        sql_query_list.append({"query_name": "uom", "sql_query": uom_query})

    # Add output query
    output_query = _build_output_query(
        sql_query_list=sql_query_list,
        to_json=summary_parameters["to_json"],
        limit=summary_parameters["limit"],
        offset=summary_parameters["offset"],
    )
    sql_query_list.append({"query_name": "output", "sql_query": output_query})
    # Build final SQL using CTE statement builder
    sql_query = _build_sql_cte_statement(sql_query_list)

    return sql_query


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

    if query_type == "sql":
        return _sql_query(parameters_dict)

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
        sample_prepared_query = _sample_query(parameters_dict)
        return sample_prepared_query

    if query_type == "interpolate":
        parameters_dict["range_join_seconds"] = _convert_to_seconds(
            parameters_dict["time_interval_rate"]
            + " "
            + parameters_dict["time_interval_unit"][0]
        )
        interpolate_prepared_query = _interpolation_query(parameters_dict)
        return interpolate_prepared_query

    if query_type == "plot":
        parameters_dict["range_join_seconds"] = _convert_to_seconds(
            parameters_dict["time_interval_rate"]
            + " "
            + parameters_dict["time_interval_unit"][0]
        )
        plot_prepared_query = _plot_query(parameters_dict)
        return plot_prepared_query

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
