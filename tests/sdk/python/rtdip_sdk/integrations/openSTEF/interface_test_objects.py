# Copyright 2023 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

RAW_WEATHER_QUERY = """	
from(bucket: "my/bucket") 
	|> range(start: 2024-01-01T12:30:30Z, stop: 2024-01-02T12:30:30Z) 
	|> filter(fn: (r) => r._measurement == "weather" and (r._field == "field_1" or r._field == "field_2") and (r.source == "source_1" or r.source == "source_2") and r.input_city == "input_city")
"""

RAW_PREDICTION_QUERY = """
from(bucket: "my/bucket")
	|> range(start: 2024-01-01T12:30:30Z, stop: 2024-01-02T12:30:30Z) 
	|> filter(fn: (r) => 
	    r._measurement == "prediction")
	|> filter(fn: (r) => 
	   r._field == "quality" )                 
	|> filter(fn: (r) => r.pid == "123")
"""

RESAMPLE_CREATE_EMPTY_FALSE = """
from(bucket: "my/bucket")
	|> range(start: 2024-01-01T12:30:30Z, stop: 2024-01-02T12:30:30Z) 
	|> filter(fn: (r) => r["_measurement"] == "sjv")
	|> filter(fn: (r) => r["_field"] == "field_1" or r["_field"] == "field_2")
	|> aggregateWindow(every: 15m, fn: mean, createEmpty: false)
	|> yield(name: "mean")
"""

RESAMPLE_CREATE_EMPTY_TRUE = """
from(bucket: "my/bucket")
	|> range(start: 2024-01-01T12:30:30Z, stop: 2024-01-02T12:30:30Z) 
	|> filter(fn: (r) => 
	    r._measurement == "prediction")
	|> filter(fn: (r) => 
	   r._field == "field_1" or r._field == "field_2")                 
	|> filter(fn: (r) => r.pid == "123")
	|> aggregateWindow(every: 15m, fn: mean)
"""

MAX_QUERY = """
from(bucket: "my/bucket") 
	|> range(start: - 2d) 
	|> filter(fn: (r) => r._measurement == "weather" and r.source == "harm_arome" and r._field == "source_run")
	|> max()
"""

PIVOT_QUERY = """
from(bucket: "my/bucket") 
                    |> range(start: 2024-01-01T12:30:30Z, stop: 2024-01-02T12:30:30Z) 
                    |> filter(fn: (r) => r._measurement == "power")
                    |> filter(fn: (r) => r._field == "output")
                    |> filter(fn: (r) => r.system == "system_1" or r.system == "system_2")
                    |> pivot(rowKey:["_time"], columnKey: ["system"], valueColumn: "_value")
"""

MULTI_RESAMPLE_QUERY = """
data = from(bucket: "my/bucket") 
                    |> range(start: 2024-01-01T12:30:30Z, stop: 2024-01-02T12:30:30Z) 
                    |> filter(fn: (r) => r._measurement == "power")
                    |> filter(fn: (r) => r._field == "output")
                    |> filter(fn: (r) => r.system == "system_1" or r.system == "system_2")
                    |> aggregateWindow(every: 3m, fn: mean)

                data
                    |> group() |> aggregateWindow(every: 3m, fn: sum)
                    |> yield(name: "load")

                data
                    |> group() |> aggregateWindow(every: 3m, fn: count)
                    |> yield(name: "nEntries")
"""

RAW_WEATHER_SQL_QUERY = [
    'WITH raw_events AS (SELECT Latitude, Longitude, EnqueuedTime, EventTime AS _time, Value AS _value, Status, Latest, EventDate, TagName, split(TagName, ":") AS tags_array, tags_array[0] AS _field, tags_array[1] AS input_city, tags_array[2] AS source, "weather" AS _measurement FROM `weather`) SELECT * FROM raw_events WHERE (_measurement == "weather" and (_field == "field_1" or _field == "field_2") and (source == "source_1" or source == "source_2") and input_city == "input_city") AND _time BETWEEN to_timestamp("2024-01-01T12:30:30") AND to_timestamp("2024-01-02T12:30:30")'
]


RAW_PREDICTION_SQL_QUERY = [
    'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, tags_array[0] AS _field, tags_array[1] AS customer, tags_array[2] AS pid, tags_array[3] AS type, "prediction" AS _measurement FROM `prediction`)SELECT * FROM raw_events WHERE (_measurement == "prediction") AND (_field == "quality" ) AND (pid == "123") AND _time BETWEEN to_timestamp("2024-01-01T12:30:30") AND to_timestamp("2024-01-02T12:30:30")'
]

RESAMPLE_CREATE_EMPTY_FALSE_SQL = [
    'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, tags_array[0] AS _field, tags_array[1] AS year_created, "sjv" AS _measurement FROM `sjv`), raw_events_filtered AS (SELECT * FROM raw_events WHERE (_measurement == "sjv") AND (_field == "field_1" or _field == "field_2") AND _time BETWEEN to_timestamp("2024-01-01T12:30:30") AND to_timestamp("2024-01-02T12:30:30")), date_array AS (SELECT explode(sequence(to_timestamp("2024-01-01T12:30:30"), to_timestamp("2024-01-02T12:30:30"), INTERVAL "15 minute")) AS timestamp_array) , window_buckets AS (SELECT TagName, WINDOW(timestamp_array, "15 minute") AS w, w.start AS window_start, w.end AS window_end FROM (SELECT DISTINCT TagName FROM raw_events_filtered) AS tag_names CROSS JOIN date_array) , resample AS (SELECT /*+ RANGE_JOIN(a, 900) */ a.TagName, window_end AS _time, mean(_value) AS _value, Status, _field, b.year_created FROM window_buckets a LEFT JOIN raw_events_filtered b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName GROUP BY ALL) , resample_results AS (SELECT "mean" AS result, _time, _value, "Good" AS Status, TagName, _field , CAST(year_created AS STRING) FROM resample WHERE _time > to_timestamp("2024-01-01T12:30:30") AND _field IS NOT NULL ORDER BY TagName, _time) SELECT * FROM resample_results '
]

RESAMPLE_CREATE_EMPTY_TRUE_SQL = [
    'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, tags_array[0] AS _field, tags_array[1] AS customer, tags_array[2] AS pid, tags_array[3] AS type, "prediction" AS _measurement FROM `prediction`), raw_events_filtered AS (SELECT * FROM raw_events WHERE (_measurement == "prediction") AND (_field == "field_1" or _field == "field_2") AND (pid == "123") AND _time BETWEEN to_timestamp("2024-01-01T12:30:30") AND to_timestamp("2024-01-02T12:30:30")), date_array AS (SELECT explode(sequence(to_timestamp("2024-01-01T12:30:30"), to_timestamp("2024-01-02T12:30:30"), INTERVAL "15 minute")) AS timestamp_array) , window_buckets AS (SELECT TagName, WINDOW(timestamp_array, "15 minute") AS w, w.start AS window_start, w.end AS window_end FROM (SELECT DISTINCT TagName FROM raw_events_filtered) AS tag_names CROSS JOIN date_array) , resample AS (SELECT /*+ RANGE_JOIN(a, 900) */ a.TagName, window_end AS _time, mean(_value) AS _value, Status, _field, b.customer, b.pid, b.type FROM window_buckets a LEFT JOIN raw_events_filtered b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName GROUP BY ALL) , fill_nulls AS (SELECT *, last_value(_field, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS _field_forward, first_value(_field, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS _field_backward , last_value(customer, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS customer_forward, first_value(customer, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS customer_backward , last_value(pid, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS pid_forward, first_value(pid, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS pid_backward , last_value(type, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS type_forward, first_value(type, true) OVER (PARTITION BY TagName ORDER BY TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) AS type_backward  FROM resample ), resample_results AS (SELECT "_result" AS result, _time, _value, "Good" AS Status, TagName, coalesce(_field_forward, _field_backward) AS _field , CAST(coalesce(customer_forward, customer_backward) AS STRING) AS customer , CAST(coalesce(pid_forward, pid_backward) AS STRING) AS pid , CAST(coalesce(type_forward, type_backward) AS STRING) AS type FROM fill_nulls WHERE _time > to_timestamp("2024-01-01T12:30:30") GROUP BY ALL ORDER BY TagName, _time) SELECT * FROM resample_results '
]

MAX_SQL_QUERY = [
    'WITH raw_events AS (SELECT Latitude, Longitude, EnqueuedTime, EventTime AS _time, Value AS _value, Status, Latest, EventDate, TagName, split(TagName, ":") AS tags_array, tags_array [0] AS _field, tags_array [1] AS input_city, tags_array [2] AS source, "weather" AS _measurement FROM `weather`), raw_events_filtered AS (SELECT * FROM raw_events WHERE (_measurement == "weather" and source == "harm_arome" and _field == "source_run") AND _time >= to_timestamp(timestampadd(day, -2, current_timestamp()))), max_events AS (SELECT _time, MAX(_value) OVER (PARTITION BY TagName) AS _value, Status, TagName, _field, _measurement, input_city, source FROM raw_events_filtered), results AS (SELECT a._time, a._value, a.Status, a.TagName, a._field, a._measurement, a.input_city, a.source, ROW_NUMBER() OVER (PARTITION BY a.TagName ORDER BY a._time) AS ordered FROM max_events a INNER JOIN raw_events_filtered b ON a._time = b._time AND a._value = b._value)SELECT _time, _value, Status, TagName, _field, input_city, source FROM results WHERE ordered = 1 ORDER BY input_city, _field, _time'
]

PIVOT_SQL_QUERY = [
    'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, tags_array [0] AS _field, tags_array [1] AS system, "power" AS _measurement FROM `power`), raw_events_filtered AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY system ORDER BY _time) AS ordered FROM raw_events WHERE (_measurement == "power") AND (_field == "output") AND (system == "system_1" or system == "system_2") AND _time BETWEEN to_timestamp("2024-01-01T12:30:30") AND to_timestamp("2024-01-02T12:30:30")), pivot_table AS (SELECT _time, Status, TagName, _field, _measurement,  first_value(system_1, true) OVER (PARTITION BY _time ORDER BY _time, TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS system_1 ,  first_value(system_2, true) OVER (PARTITION BY _time ORDER BY _time, TagName ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS system_2  FROM raw_events_filtered PIVOT (MAX(_value) FOR system IN ( "system_1" ,  "system_2"  )) ORDER BY TagName, _time)  SELECT _time, Status, system_1, system_2 FROM pivot_table WHERE  system_1 IS NOT NULL AND  system_2 IS NOT NULL  ORDER BY _time'
]

MULTI_RESAMPLE_SQL_QUERY = [
    'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, tags_array[0] AS _field, tags_array[1] AS system, "power" AS _measurement FROM `power`), raw_events_filtered AS (SELECT * FROM raw_events WHERE (_measurement == "power") AND (_field == "output") AND (system == "system_1" or system == "system_2") AND _time BETWEEN to_timestamp("2024-01-01T12:30:30") AND to_timestamp("2024-01-02T12:30:30")), date_array AS (SELECT explode(sequence(to_timestamp("2024-01-01T12:30:30"), to_timestamp("2024-01-02T12:30:30"), INTERVAL "3 minute")) AS timestamp_array) , window_buckets AS (SELECT TagName, WINDOW(timestamp_array, "3 minute") AS w, w.start AS window_start, w.end AS window_end FROM (SELECT DISTINCT TagName FROM raw_events_filtered) AS tag_names CROSS JOIN date_array) , resample AS (SELECT /*+ RANGE_JOIN(a, 180) */ a.TagName, window_end AS _time, mean(_value) AS _value, Status, _field, b.system FROM window_buckets a LEFT JOIN raw_events_filtered b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName GROUP BY ALL) , resample_sum AS (SELECT /*+ RANGE_JOIN(a, 180) */ "load" AS result, _time, sum(_value) AS _value, "Good" AS Status FROM window_buckets a LEFT JOIN resample b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName WHERE  _time < to_timestamp("2024-01-02T12:30:30") GROUP BY ALL), resample_count AS (SELECT /*+ RANGE_JOIN(a, 180) */ "nEntries" AS result, _time, count(*) AS _value, "Good" AS Status FROM window_buckets a LEFT JOIN resample b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName WHERE  _time < to_timestamp("2024-01-02T12:30:30") GROUP BY ALL) SELECT * FROM resample_sum ORDER BY _time',
    'WITH raw_events AS (SELECT EventTime AS _time, Value AS _value, Status, TagName, split(TagName, ":") AS tags_array, tags_array[0] AS _field, tags_array[1] AS system, "power" AS _measurement FROM `power`), raw_events_filtered AS (SELECT * FROM raw_events WHERE (_measurement == "power") AND (_field == "output") AND (system == "system_1" or system == "system_2") AND _time BETWEEN to_timestamp("2024-01-01T12:30:30") AND to_timestamp("2024-01-02T12:30:30")), date_array AS (SELECT explode(sequence(to_timestamp("2024-01-01T12:30:30"), to_timestamp("2024-01-02T12:30:30"), INTERVAL "3 minute")) AS timestamp_array) , window_buckets AS (SELECT TagName, WINDOW(timestamp_array, "3 minute") AS w, w.start AS window_start, w.end AS window_end FROM (SELECT DISTINCT TagName FROM raw_events_filtered) AS tag_names CROSS JOIN date_array) , resample AS (SELECT /*+ RANGE_JOIN(a, 180) */ a.TagName, window_end AS _time, mean(_value) AS _value, Status, _field, b.system FROM window_buckets a LEFT JOIN raw_events_filtered b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName GROUP BY ALL) , resample_sum AS (SELECT /*+ RANGE_JOIN(a, 180) */ "load" AS result, _time, sum(_value) AS _value, "Good" AS Status FROM window_buckets a LEFT JOIN resample b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName WHERE  _time < to_timestamp("2024-01-02T12:30:30") GROUP BY ALL), resample_count AS (SELECT /*+ RANGE_JOIN(a, 180) */ "nEntries" AS result, _time, count(*) AS _value, "Good" AS Status FROM window_buckets a LEFT JOIN resample b ON a.window_start <= b._time AND a.window_end > b._time AND a.TagName = b.TagName WHERE  _time < to_timestamp("2024-01-02T12:30:30") GROUP BY ALL) SELECT * FROM resample_count ORDER BY _time',
]
