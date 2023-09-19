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
from jinja2 import Template

def _build_parameters(query):
    columns = {
        "weather": ['input_city', 'source'],
        "power": ['system'],
        "prediction_taheads": ['customer', 'pid', 'tAhead', 'type']
    }

    parameters = {}

    measurement_pattern = r'r\._measurement == "([^"]+)"'
    table = re.search(measurement_pattern, query).group(1)
    parameters["table"] = table
    parameters["columns"] = columns[parameters["table"].lower()]

    start = re.search(r"start:\s+([^|,Z]+)", query).group(1)
    stop = re.search(r"stop:\s+([^|Z)]+)", query).group(1).strip()
    parameters["start"] = start
    parameters["stop"] = stop

    window_pattern = r"\|> aggregateWindow\((.*)\)"
    window = re.findall(window_pattern, query)
    if window:
        every = re.findall(r"every: ([^,]+)m", str(window))
        parameters["time_interval_rate"] = every

        fn = re.findall(r"fn: ([^,\]']+)", str(window))
        parameters["agg_method"] = fn

        parameters["time_interval_unit"] = "minute"
        parameters["range_join_seconds"] = int(parameters["time_interval_rate"][0]) * 60

    filter_sections = re.findall(r"\|> filter\(fn: \(r\) => (.*?)(?=\s*\||$)", query, re.DOTALL)
    filter = " AND ".join(["(" + i.strip() for i in filter_sections])

    where = re.sub(r"r\.", "", filter)
    if where.count("(") != where.count(")"):
        where = "(" + where
        
    parameters["where"] = where

    yields = re.findall(r"\|> yield\(name: \"(.*?)\"\)", query)
    if yields:
        parameters["yield"] = yields

    createEmpty = re.search(r"createEmpty: (.*?)\)", query)
    parameters["createEmpty"] = 'true'
    if createEmpty:
        parameters["createEmpty"] = createEmpty.group(1)

    return parameters

def _raw_query(query: str) -> list:
    parameters = _build_parameters(query)

    flux_query = (
        "WITH raw_events AS (SELECT " 
        "result, DENSE_RANK() OVER (ORDER BY table) - 1 AS table, to_timestamp(\"{{ start }}\") AS _start, to_timestamp(\"{{ stop }}\") AS _stop, _time, _value, _field, _measurement, " 
        "{% for col in columns %}"
        "{{ col }}"
        "{% if not loop.last %}"
        ", "
        "{% endif %}"
        "{% endfor %}"
        " FROM `{{ table }}` "
        "WHERE {{ where }} "
        "AND _time BETWEEN to_timestamp(\"{{ start }}\") AND to_timestamp(\"{{ stop }}\")) "
        
        "{% if table == 'weather'%}"
        "SELECT lat AS Latitude, lon AS Longitude, current_timestamp() AS EnqueuedTime, _time AS EventTime, _value AS Value, source AS Source, \"Good\" AS Status, True AS Latest, date(_time) AS EventDate, concat(_field, \":\", input_city, \":\", source) AS TagName, _time, _field, _value, input_city, source FROM raw_events a INNER JOIN nametolatlon b ON a.input_city = b.regionInput ORDER BY TagName, EventTime"
        "{% else %}"
        "SELECT _time AS EventTime, _value AS Value, \"Good\" AS Status, concat(_field"
        "{% for col in columns %}"
        ", \":\", {{ col }}"
        "{% endfor %}"
        ") AS TagName, _time, _field, _value"
        "{% for col in columns %}"
        ", {{ col }}"
        "{% endfor %}"
        " FROM raw_events "
        "ORDER BY TagName, EventTime"
        "{% endif %}"
        )  
    
    sql_template = Template(flux_query)
    sql_query = sql_template.render(parameters)
    return [sql_query]

def _resample_query(query: str) -> str:
    parameters = _build_parameters(query)
    parameters["filters"] = re.findall(r'r\.system == "([^"]+)"', query)

    resample_base_query = (
        "WITH raw_events AS (SELECT * FROM `{{ table }}` WHERE {{ where }} AND _time BETWEEN to_timestamp(\"{{ start }}\") AND to_timestamp(\"{{ stop }}\")) "
        ", date_array AS (SELECT DISTINCT table, explode(sequence(to_timestamp(\"{{ start }}\"), to_timestamp(\"{{ stop }}\"), INTERVAL \"{{ time_interval_rate[0] + \" \" + time_interval_unit }}\")) AS timestamp_array FROM raw_events) "
        ", date_intervals AS (SELECT table, from_unixtime(floor(unix_timestamp(timestamp_array) / ({{ time_interval_rate[0] }} * 60)) * ({{ time_interval_rate[0] }} * 60), \"yyyy-MM-dd HH:mm:ss\") AS timestamp_array FROM date_array) "
        ", window_buckets AS (SELECT table, timestamp_array AS window_start, timestampadd({{ time_interval_unit }}, {{ time_interval_rate[0] }}, timestamp_array) as window_end FROM date_intervals) "
        ", resample AS ( SELECT /*+ RANGE_JOIN(a, {{ range_join_seconds }}) */ b.result, a.table, b._start, b._stop, a.window_start, a.window_end, b._value, b._field, b._measurement "
        "{% for col in columns if columns is defined and columns|length > 0 %}"
        ", b.{{ col }}"
        "{% endfor %}"
        " FROM window_buckets a FULL OUTER JOIN raw_events b ON a.window_start <= b._time AND a.window_end > b._time AND a.table = b.table) "   
    )

    if len(re.findall(r'\|> aggregateWindow', query)) == 1:
        flux_query = (
            f"{resample_base_query}"
            "{% if createEmpty == 'true' %}"
            ", fill_nulls AS (SELECT *, last_value(_field, true) OVER (PARTITION BY table ORDER BY table ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _field_forward, first_value(_field, true) OVER (PARTITION BY table ORDER BY table ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS _field_backward, "
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            "last_value({{ col }}, true) OVER (PARTITION BY table ORDER BY table ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {{ col }}_forward, first_value({{ col }}, true) OVER (PARTITION BY table ORDER BY table ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS {{ col }}_backward, "
            "{% endfor %}"
            " DENSE_RANK() OVER (ORDER BY table) - 1 AS rank_table FROM resample) "  
            "{% if yield is defined and yields|length > 0 %}"
            " , resample_results AS (SELECT \"{{ yield }}\" AS result, " 
            "{% else %}"
            " , resample_results AS (SELECT \"_result\" AS result, "
            "{% endif %}"
            "rank_table AS table, to_timestamp(\"{{ start }}\") AS _start, to_timestamp(\"{{ stop }}\") AS _stop, window_end AS _time, {{ agg_method[0] }}(_value) AS _value, coalesce(_field_forward, _field_backward) AS _field, \"{{ table }}\" AS _measurement "
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            ", CAST(coalesce({{ col }}_forward, {{ col }}_backward) AS STRING) AS {{ col }} "
            "{% endfor %}"
            "FROM fill_nulls WHERE window_end BETWEEN to_timestamp(\"{{ start }}\") AND to_timestamp(\"{{ stop }}\") GROUP BY result, rank_table, _start, _stop, _time, coalesce(_field_forward, _field_backward), _measurement "
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            ", coalesce({{ col }}_forward, {{ col }}_backward) "
            "{% endfor %}"
            "ORDER BY table, _time) "

            "{% else %}"
            "{% if yield is defined and yields|length > 0 %}"
            " , resample_results AS SELECT \"{{ yield }}\" AS result, " 
            "{% else %}"
            " , resample_results AS (SELECT \"_result\" AS result, "
            "{% endif %}"
            "DENSE_RANK() OVER (ORDER BY table) - 1 AS table, to_timestamp(\"{{ start }}\") AS _start, to_timestamp(\"{{ stop }}\") AS _stop, window_end AS _time, {{ agg_method[0] }}(_value) AS _value,  _field, \"{{ table }}\" AS _measurement "
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            ", CAST({{ col }} AS STRING) "
            "{% endfor %}"
            "FROM resample WHERE window_end BETWEEN to_timestamp(\"{{ start }}\") AND to_timestamp(\"{{ stop }}\") AND result IS NOT NULL GROUP BY result, table, _start, _stop, _time, _field, _measurement "
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            ", {{ col }} "
            "{% endfor %}"
            "ORDER BY table, _time) "
            "{% endif %}"
        )

        flux_query = (
            f"{flux_query}"
            "{% if table == 'weather'%}"
            "SELECT lat AS Latitude, lon AS Longitude, current_timestamp() AS EnqueuedTime, _time AS EventTime, _value AS Value, source AS Source, \"Good\" AS Status, True AS Latest, date(_time) AS EventDate, concat(_field, \":\", input_city, \":\", source) AS TagName, _time, _field, _value, input_city, source FROM resample_results a INNER JOIN nametolatlon b ON a.input_city = b.regionInput ORDER BY TagName, EventTime"
            "{% else %}"
            "SELECT _time AS EventTime, _value AS Value, \"Good\" AS Status, concat(_field"
            "{% for col in columns %}"
            ", \":\", {{ col }}"
            "{% endfor %}"
            ") AS TagName, _time, _field, _value"
            "{% for col in columns %}"
            ", {{ col }}"
            "{% endfor %}"
            " FROM resample_results "
            "ORDER BY TagName, EventTime"
            "{% endif %}"
        )

        sql_template = Template(flux_query)
        sql_query = sql_template.render(parameters)
        return [sql_query]

    elif len(re.findall(r'\|> aggregateWindow', query)) > 1:
        sql_query = (
            f"{resample_base_query}"
            ", fill_nulls AS (SELECT *, last_value(_field, true) OVER (PARTITION BY table ORDER BY table ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _field_forward, first_value(_field, true) OVER (PARTITION BY table ORDER BY table ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS _field_backward, "
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            "last_value({{ col }}, true) OVER (PARTITION BY table ORDER BY table ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {{ col }}_forward, first_value({{ col }}, true) OVER (PARTITION BY table ORDER BY table ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS {{ col }}_backward, "
            "{% endfor %}"
            " DENSE_RANK() OVER (ORDER BY table) - 1 AS rank_table FROM resample) " 
            ", resample_results AS (SELECT \"_result\" AS result, rank_table AS table, to_timestamp(\"{{ start }}\") AS _start, to_timestamp(\"{{ stop }}\") AS _stop, window_end AS _time, {{ agg_method[0] }}(_value) AS _value, coalesce(_field_forward, _field_backward) AS _field, \"{{ table }}\" AS _measurement, CAST(system AS STRING) FROM fill_nulls GROUP BY result, rank_table, _start, _stop, _time, coalesce(_field_forward, _field_backward), _measurement, system ORDER BY table, _time) "
            ", date_array_2 AS (SELECT DISTINCT table, explode(sequence(to_timestamp(\"{{ start }}\"), timestampadd({{ time_interval_unit }}, {{ time_interval_rate[0] }}, to_timestamp(\"{{ stop }}\")), INTERVAL \"{{ time_interval_rate[0] + \" \" + time_interval_unit }}\")) AS timestamp_array FROM resample_results) "
            ", date_intervals_2 AS (SELECT table, from_unixtime(floor(unix_timestamp(timestamp_array) / ({{ time_interval_rate[0] }} * 60)) * ({{ time_interval_rate[0] }} * 60), \"yyyy-MM-dd HH:mm:ss\") AS timestamp_array FROM date_array_2) "
            ", window_buckets_2 AS (SELECT table, timestamp_array AS window_start, timestampadd({{ time_interval_unit }}, {{ time_interval_rate[0] }}, timestamp_array) as window_end FROM date_intervals_2) "
            ", project_resample_results AS (SELECT /*+ RANGE_JOIN(a, {{ range_join_seconds }}) */ b.result, a.table, a.window_end AS _time, b._start, b._stop, b._value FROM window_buckets_2 a FULL OUTER JOIN resample_results b ON a.window_start <= b._time AND a.window_end > b._time AND a.table = b.table) "
            ", resample_sum AS (SELECT _time AS EventTime, sum(_value) AS Value, \"Good\" AS Status, concat(_field" 
            "{% for filter in filters %}"
            ", \":\", \"{{ filter }}\" "
            "{% endfor %}"
            ") AS TagName, _time, sum(_value) AS _value FROM project_resample_results WHERE _time BETWEEN to_timestamp(\"{{ start }}\") AND to_timestamp(\"{{ stop }}\") GROUP BY _time, _field ORDER BY EventTime)"
            ", resample_count AS (SELECT _time AS EventTime, count(_value) AS Value, \"Good\" AS Status, concat(_field" 
            "{% for filter in filters %}"
            ", \":\", \"{{ filter }}\" "
            "{% endfor %}"
            ") AS TagName, _time, count(_value) AS _value FROM project_resample_results WHERE _time BETWEEN to_timestamp(\"{{ start }}\") AND to_timestamp(\"{{ stop }}\") GROUP BY _time, _field ORDER BY EventTime)"
        )

        sum_query = (
            f"{sql_query}"
            " SELECT * FROM resample_sum "
        )

        count_query = (
            f"{sql_query}"
            " SELECT * FROM resample_count "
        )

        sum_template = Template(sum_query)
        sum_query = sum_template.render(parameters)

        count_template = Template(count_query)
        count_query = count_template.render(parameters)
        
        return [sum_query, count_query]

def _pivot_query(query: str) -> str:
    parameters = _build_parameters(query)
    parameters["filters"] = re.findall(r'r\.system == "([^"]+)"', query)

    flux_query = (
        "WITH raw_events AS (SELECT * FROM `power` WHERE {{ where }} AND _time >= to_timestamp(\"{{ start }}\") AND _time <= to_timestamp(\"{{ stop }}\")) "
        ", pivot_table AS (SELECT result, 0 AS table, to_timestamp(\"{{ start }}\") AS _start, to_timestamp(\"{{ stop }}\") AS _stop, _time, _field, _measurement, "
        "{% for filter in filters %}"
        " first_value({{ filter }}, true) OVER (PARTITION BY _time ORDER BY _time, table ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS {{ filter }} "
        "{% if not loop.last %}"
        ", "
        "{% endif %}"
        "{% endfor %}"
        " FROM raw_events PIVOT (MAX(_value) FOR system IN ("
        "{% for filter in filters %}"
        " \"{{ filter }}\" "
        "{% if not loop.last %}"
        ", "
        "{% endif %}"
        "{% endfor %}"
        " )) ORDER BY table, _time) "
        " SELECT _time AS EventTime" 
        "{% for filter in filters %}"
        ", filter"
        "{% endfor %}"
        ", _time FROM pivot_table WHERE "
        "{% for filter in filters %}"
        " {{ filter }} IS NOT NULL "
        "{% if not loop.last %}"
        "AND "
        "{% endif %}"
        "{% endfor %}"
        " ORDER BY _time"
    )

    sql_template = Template(flux_query)
    sql_query = sql_template.render(parameters)
    return [sql_query]

def _max_query() -> str:
    sql_query = (
        "WITH raw_events AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY table ORDER BY _time) AS numbered_rows FROM `weather` WHERE (_field == 'source_run' AND source == 'harm_arome') AND _time >= to_timestamp(timestampadd(day, -2, current_timestamp())))"
        ", max_events AS (SELECT *, MAX(_value) OVER (PARTITION BY table) AS max_value FROM raw_events WHERE numbered_rows <= 10)"
        ", results AS (SELECT a.*, ROW_NUMBER() OVER (PARTITION BY a.table ORDER BY a._time) AS ordered_rows FROM max_events a INNER JOIN raw_events b ON a._time = b._time AND a.max_value = b._value)"
        "SELECT _time AS EventTime, _value AS Value, \"Good\" AS Status, concat(_field, \":\", system) AS TagName, _value FROM results WHERE ordered_rows = 1"    
    )
    
    return [sql_query]

def _query_builder(query: str) -> str:
    if re.search(r'\|> aggregateWindow', query):
        return _resample_query(query)

    elif re.search(r'\|> pivot', query):
        return _pivot_query(query)

    elif re.search(r'\|> max\(\)', query):
        return _max_query()

    else:
        return _raw_query(query)