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

aggregate_window = r"\|> aggregateWindow"


def _build_parameters(query):
    columns = {
        "weather": ["input_city", "source"],
        "power": ["system"],
        "prediction_taheads": ["customer", "pid", "tAhead", "type"],
        "prediction": ["customer", "pid", "type"],
        "marketprices": ["Name"],
        "sjv": ["year_created"],
    }

    parameters = {}

    measurement_pattern = r'r(?:\._measurement|(\["_measurement"\]))\s*==\s*"([^"]+)"'
    table = re.search(measurement_pattern, query).group(2)
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

    filter_sections = re.findall(
        r"\|> filter\(fn: \(r\) => ([^|]*)(?=\s*\||$)", query, re.DOTALL
    )
    _filter = " AND ".join(["(" + i.strip() for i in filter_sections])

    where = re.sub(r'r\.([\w]+)|r\["([^"]+)"\]', r"\1\2", _filter)
    if where.count("(") != where.count(")"):
        where = "(" + where

    parameters["where"] = where

    yields = re.findall(r"\|> yield\(name: \"(.*?)\"\)", query)
    if yields:
        parameters["yield"] = yields

    create_empty = re.search(r"createEmpty: (.*?)\)", query)
    parameters["createEmpty"] = "true"
    if create_empty:
        parameters["createEmpty"] = create_empty.group(1)

    return parameters


def _raw_query(query: str) -> list:
    parameters = _build_parameters(query)

    flux_query = (
        "{% if table == 'weather'%}"
        'WITH raw_events AS (SELECT Latitude, Longitude, EnqueuedTime, EventTime AS _time, Value AS _value, Status, Latest, EventDate, TagName, split(TagName, ":") AS tags_array, tags_array[0] AS _field, tags_array[1] AS input_city, tags_array[2] AS source, "weather" AS _measurement FROM `weather`) '
        "{% else %}"
        'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, '
        "tags_array[0] AS _field, "
        "{% for col in columns %}"
        "tags_array[{{ columns.index(col) + 1 }}] AS {{ col }}, "
        "{% endfor %}"
        '"{{ table }}" AS _measurement FROM `{{ table }}`)'
        "{% endif %}"
        'SELECT * FROM raw_events WHERE {{ where }} AND _time BETWEEN to_timestamp("{{ start }}") AND to_timestamp("{{ stop }}")'
    )

    sql_template = Template(flux_query)
    sql_query = sql_template.render(parameters)
    return [sql_query]


def _resample_query(query: str) -> list:
    parameters = _build_parameters(query)
    parameters["filters"] = re.findall(r'r\.system == "([^"]+)"', query)

    resample_base_query = (
        "{% if table == 'weather'%}"
        'WITH raw_events AS (SELECT Latitude, Longitude, EnqueuedTime, EventTime AS _time, Value AS _value, Status, Latest, EventDate, TagName, split(TagName, ":") AS tags_array, tags_array[0] AS _field, tags_array[1] AS input_city, tags_array[2] AS source, "weather" AS _measurement FROM `weather`) '
        "{% else %}"
        'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, '
        "tags_array[0] AS _field, "
        "{% for col in columns %}"
        "tags_array[{{ columns.index(col) + 1 }}] AS {{ col }}, "
        "{% endfor %}"
        '"{{ table }}" AS _measurement FROM `{{ table }}`)'
        "{% endif %}"
        ', raw_events_filtered AS (SELECT * FROM raw_events WHERE {{ where }} AND _time BETWEEN to_timestamp("{{ start }}") AND to_timestamp("{{ stop }}"))'
        ', date_array AS (SELECT explode(sequence(to_timestamp("{{ start }}"), to_timestamp("{{ stop }}"), INTERVAL "{{ time_interval_rate[0] + " " + time_interval_unit }}")) AS timestamp_array) '
        ', window_buckets AS (SELECT TagName, WINDOW(timestamp_array, "{{ time_interval_rate[0] + " " + time_interval_unit }}") AS w, w.start AS window_start, w.end AS window_end FROM (SELECT DISTINCT TagName FROM raw_events_filtered) AS tag_names CROSS JOIN date_array) '
        ", resample AS (SELECT /*+ RANGE_JOIN(a, {{ range_join_seconds }}) */ a.TagName, window_end AS _time, {{ agg_method[0] }}(_value) AS _value, Status, _field"
        "{% for col in columns if columns is defined and columns|length > 0 %}"
        ", b.{{ col }}"
        "{% endfor %}"
        " FROM window_buckets a LEFT JOIN raw_events_filtered b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName GROUP BY ALL) "
    )

    if len(re.findall(aggregate_window, query)) == 1:
        flux_query = (
            f"{resample_base_query}"
            "{% if createEmpty == 'true' %}"
            ", fill_nulls AS (SELECT *, last_value(_field, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _field_forward, first_value(_field, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS _field_backward "
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            ", last_value({{ col }}, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS {{ col }}_forward, first_value({{ col }}, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS {{ col }}_backward "
            "{% endfor %}"
            " FROM resample"
            "{% if yield is defined and yield|length > 0 %}"
            ' ), resample_results AS (SELECT "{{ yield[0] }}" AS result, '
            "{% else %}"
            ' ), resample_results AS (SELECT "_result" AS result, '
            "{% endif %}"
            '_time, _value, "Good" AS Status, TagName, coalesce(_field_forward, _field_backward) AS _field '
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            ", CAST(coalesce({{ col }}_forward, {{ col }}_backward) AS STRING) AS {{ col }} "
            "{% endfor %}"
            'FROM fill_nulls WHERE _time > to_timestamp("{{ start }}") GROUP BY ALL '
            "ORDER BY TagName, _time) "
            "{% else %}"
            "{% if yield is defined and yield|length > 0 %}"
            ', resample_results AS (SELECT "{{ yield[0] }}" AS result, '
            "{% else %}"
            ', resample_results AS (SELECT "_result" AS result, '
            "{% endif %}"
            '_time, _value, "Good" AS Status, TagName, _field '
            "{% for col in columns if columns is defined and columns|length > 0 %}"
            ", CAST({{ col }} AS STRING) "
            "{% endfor %}"
            'FROM resample WHERE _time > to_timestamp("{{ start }}") AND _field IS NOT NULL '
            "ORDER BY TagName, _time) "
            "{% endif %}"
        )

        flux_query = f"{flux_query}" "SELECT * FROM resample_results "

        sql_template = Template(flux_query)
        sql_query = sql_template.render(parameters)
        return [sql_query]

    elif len(re.findall(aggregate_window, query)) > 1:
        sql_query = (
            f"{resample_base_query}"
            ', resample_sum AS (SELECT /*+ RANGE_JOIN(a, {{ range_join_seconds }}) */ "load" AS result, _time, sum(_value) AS _value, "Good" AS Status FROM window_buckets a LEFT JOIN resample b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName WHERE  _time < to_timestamp("{{ stop }}") GROUP BY ALL)'
            ', resample_count AS (SELECT /*+ RANGE_JOIN(a, {{ range_join_seconds }}) */ "nEntries" AS result, _time, count(*) AS _value, "Good" AS Status FROM window_buckets a LEFT JOIN resample b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName WHERE  _time < to_timestamp("{{ stop }}") GROUP BY ALL)'
        )

        sum_query = f"{sql_query}" " SELECT * FROM resample_sum ORDER BY _time"

        count_query = f"{sql_query}" " SELECT * FROM resample_count ORDER BY _time"

        sum_template = Template(sum_query)
        sum_query = sum_template.render(parameters)

        count_template = Template(count_query)
        count_query = count_template.render(parameters)
        return [sum_query, count_query]


def _pivot_query(query: str) -> list:
    parameters = _build_parameters(query)
    parameters["filters"] = re.findall(r'r\.system == "([^"]+)"', query)

    flux_query = (
        'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, tags_array [0] AS _field, tags_array [1] AS system, "power" AS _measurement FROM `power`)'
        ', raw_events_filtered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY system ORDER BY _time) AS ordered FROM raw_events WHERE {{ where }} AND _time BETWEEN to_timestamp("{{ start }}") AND to_timestamp("{{ stop }}"))'
        ", pivot_table AS (SELECT _time, Status, TagName, _field, _measurement, "
        "{% for filter in filters %}"
        " first_value({{ filter }}, true) OVER (PARTITION BY _time ORDER BY _time, TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS {{ filter }} "
        "{% if not loop.last %}"
        ", "
        "{% endif %}"
        "{% endfor %}"
        " FROM raw_events_filtered PIVOT (MAX(_value) FOR system IN ("
        "{% for filter in filters %}"
        ' "{{ filter }}" '
        "{% if not loop.last %}"
        ", "
        "{% endif %}"
        "{% endfor %}"
        " )) ORDER BY TagName, _time) "
        " SELECT _time, Status"
        "{% for filter in filters %}"
        ", {{ filter }}"
        "{% endfor %}"
        " FROM pivot_table WHERE "
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


def _max_query() -> list:
    sql_query = (
        'WITH raw_events AS (SELECT Latitude, Longitude, EnqueuedTime, EventTime AS _time, Value AS _value, Status, Latest, EventDate, TagName, split(TagName, ":") AS tags_array, tags_array [0] AS _field, tags_array [1] AS input_city, tags_array [2] AS source, "weather" AS _measurement FROM `weather`)'
        ', raw_events_filtered AS (SELECT * FROM raw_events WHERE (_measurement == "weather" and source == "harm_arome" and _field == "source_run") AND _time >= to_timestamp(timestampadd(day, -2, current_timestamp())))'
        ", max_events AS (SELECT _time, MAX(_value) OVER (PARTITION BY TagName) AS _value, Status, TagName, _field, _measurement, input_city, source FROM raw_events_filtered)"
        ", results AS (SELECT a._time, a._value, a.Status, a.TagName, a._field, a._measurement, a.input_city, a.source, ROW_NUMBER() OVER (PARTITION BY a.TagName ORDER BY a._time) AS ordered FROM max_events a INNER JOIN raw_events_filtered b ON a._time = b._time AND a._value = b._value)"
        "SELECT _time, _value, Status, TagName, _field, input_city, source FROM results WHERE ordered = 1 ORDER BY input_city, _field, _time"
    )

    return [sql_query]


def _query_builder(query: str) -> list:
    if re.search(aggregate_window, query):
        return _resample_query(query)

    elif re.search(r"\|> pivot", query):
        return _pivot_query(query)

    elif re.search(r"\|> max\(\)", query):
        return _max_query()

    else:
        return _raw_query(query)
