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


from jinjasql import JinjaSql
from six import string_types
from copy import deepcopy
import datetime
from datetime import datetime, time

def _is_date_format(dt, format):
    try:
        return datetime.strptime(dt , format)
    except:
        return False

def _parse_date(dt, is_end_date=False, exclude_date_format=False):   
    if isinstance(dt, datetime):
        if dt.time() == time.min:
            dt = dt.date()
        else:
            dt = datetime.strftime(dt, "%Y-%m-%dT%H:%M:%S%z")
    dt = str(dt)

    if _is_date_format(dt, "%Y-%m-%d") and exclude_date_format == False:
        _time = "T23:59:59" if is_end_date == True else "T00:00:00"
        return dt + _time + "+0000"
    elif _is_date_format(dt, "%Y-%m-%dT%H:%M:%S"):
        return dt + "+0000"
    elif _is_date_format(dt, "%Y-%m-%dT%H:%M:%S%z"):
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
        parsed_timestamp = [_parse_date(dt, is_end_date=False, exclude_date_format=True) for dt in parameters_dict["timestamps"]]
        parameters_dict["timestamps"] = parsed_timestamp
        sample_dt = parsed_timestamp[0]

    parameters_dict["time_zone"] = datetime.strptime(sample_dt, "%Y-%m-%dT%H:%M:%S%z").strftime("%z")
    
    return parameters_dict

def _quote_sql_string(value):
    '''
    If `value` is a string type, escapes single quotes in the string
    and returns the string enclosed in single quotes.
    '''
    if isinstance(value, string_types):
        new_value = str(value)
        new_value = new_value.replace("'", "''")
        return "'{}'".format(new_value)
    return value


def _get_sql_from_template(query: str, bind_params: dict) -> str:
    '''
    Given a query and binding parameters produced by JinjaSql's prepare_query(),
    produce and return a complete SQL query string.
    '''
    if not bind_params:
        return query
    params = deepcopy(bind_params)
    for key, val in params.items():
        params[key] = _quote_sql_string(val)
    return query % params


def _raw_query(parameters_dict: dict) -> str:

    raw_query = (
        "SELECT from_utc_timestamp(EventTime, \"{{ time_zone | sqlsafe }}\") as EventTime, TagName, Status, Value FROM "
        "{{ business_unit | sqlsafe }}.sensors.{{ asset | sqlsafe }}_{{ data_security_level | sqlsafe }}_events_{{ data_type | sqlsafe }} "
        "WHERE EventDate BETWEEN to_date(to_timestamp({{ start_date }})) AND to_date(to_timestamp({{ end_date }})) AND EventTime BETWEEN to_timestamp({{ start_date }}) AND to_timestamp({{ end_date }}) AND TagName in {{ tag_names | inclause }} "
        "{% if include_bad_data is defined and include_bad_data == false %}"
        "AND Status = 'Good'"
        "{% endif %}"
        "ORDER BY EventTime"
    )

    raw_parameters = {
        "business_unit": parameters_dict['business_unit'].lower(),
        "region": parameters_dict['region'].lower(),
        "asset": parameters_dict['asset'].lower(),
        "data_security_level": parameters_dict['data_security_level'].lower(),
        "data_type": parameters_dict['data_type'].lower(),
        "start_date": parameters_dict['start_date'],
        "end_date": parameters_dict['end_date'],
        "tag_names": list(dict.fromkeys(parameters_dict['tag_names'])),
        "include_bad_data": parameters_dict['include_bad_data'],
        "time_zone": parameters_dict["time_zone"]
    }

    sql_template = JinjaSql(param_style='pyformat')
    query, bind_params = sql_template.prepare_query(raw_query, raw_parameters)
    sql_query = _get_sql_from_template(query, bind_params)
    return sql_query

def _sample_query(parameters_dict: dict) -> tuple:

    sample_query = (
        "SELECT DISTINCT TagName, w.start AS EventTime, {{ agg_method | sqlsafe }}(Value) OVER "
        "(PARTITION BY TagName, w.start ORDER BY EventTime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS Value FROM (" +
        "SELECT from_utc_timestamp(EventTime, \"{{ time_zone | sqlsafe }}\") as EventTime, WINDOW(from_utc_timestamp(EventTime, \"{{ time_zone | sqlsafe }}\"), {{ sample_rate + ' ' + sample_unit }}) w, TagName, Status, Value FROM "
        "{{ business_unit | sqlsafe }}.sensors.{{ asset | sqlsafe }}_{{ data_security_level | sqlsafe }}_events_{{ data_type | sqlsafe }} "
        "WHERE EventDate BETWEEN to_date(to_timestamp({{ start_date }})) AND to_date(to_timestamp({{ end_date }})) AND EventTime BETWEEN to_timestamp({{ start_date }}) AND to_timestamp({{ end_date }}) AND TagName in {{ tag_names | inclause }} "
        "{% if include_bad_data is defined and include_bad_data == false %}"
        "AND Status = 'Good'"
        "{% endif %}"
        ")"      
    )

    sample_parameters = {
        "business_unit": parameters_dict['business_unit'].lower(),
        "region": parameters_dict['region'].lower(),
        "asset": parameters_dict['asset'].lower(),
        "data_security_level": parameters_dict['data_security_level'].lower(),
        "data_type": parameters_dict['data_type'].lower(),
        "start_date": parameters_dict['start_date'],
        "end_date": parameters_dict['end_date'],
        "tag_names": list(dict.fromkeys(parameters_dict['tag_names'])),
        "include_bad_data": parameters_dict['include_bad_data'],
        "sample_rate": parameters_dict['sample_rate'],
        "sample_unit": parameters_dict['sample_unit'],
        "agg_method": parameters_dict['agg_method'],
        "time_zone": parameters_dict["time_zone"]
    }

    sql_template = JinjaSql(param_style='pyformat')
    query, bind_params = sql_template.prepare_query(sample_query, sample_parameters)
    sql_query = _get_sql_from_template(query, bind_params)
    return sql_query, sample_query, sample_parameters    

def _interpolation_query(parameters_dict: dict, sample_query: str, sample_parameters: dict) -> str:

    if parameters_dict["interpolation_method"] == "forward_fill":
        interpolation_method = 'last_value/UNBOUNDED PRECEDING/CURRENT ROW'

    if parameters_dict["interpolation_method"] == "backward_fill":
        interpolation_method = 'first_value/CURRENT ROW/UNBOUNDED FOLLOWING'

    interpolation_options = interpolation_method.split('/')

    interpolate_query = (
        "SELECT a.EventTime, a.TagName, {{ interpolation_options_0 | sqlsafe }}(b.Value, true) OVER (PARTITION BY a.TagName ORDER BY a.EventTime ROWS BETWEEN {{ interpolation_options_1 | sqlsafe }} AND {{ interpolation_options_2 | sqlsafe }}) AS Value FROM "
        "(SELECT explode(sequence(from_utc_timestamp(to_timestamp({{ start_date }}), \"{{ time_zone | sqlsafe }}\"), from_utc_timestamp(to_timestamp({{ end_date }}), \"{{ time_zone | sqlsafe }}\"), INTERVAL {{ sample_rate + ' ' + sample_unit }})) AS EventTime, "
        "explode(array{{ tag_names | inclause }}) AS TagName) a "
        f"LEFT OUTER JOIN ({sample_query}) b "
        "ON a.EventTime = b.EventTime "
        "AND a.TagName = b.TagName"        
    )
    
    interpolate_parameters = sample_parameters.copy()
    interpolate_parameters["interpolation_options_0"] = interpolation_options[0]
    interpolate_parameters["interpolation_options_1"] = interpolation_options[1]
    interpolate_parameters["interpolation_options_2"] = interpolation_options[2]

    sql_template = JinjaSql(param_style='pyformat')
    query, bind_params = sql_template.prepare_query(interpolate_query, interpolate_parameters)
    sql_query = _get_sql_from_template(query, bind_params)
    return sql_query

def _interpolation_at_time(parameters_dict: dict) -> str:
    timestamps_deduplicated = list(dict.fromkeys(parameters_dict['timestamps'])) #remove potential duplicates in tags
    parameters_dict["timestamps"] = timestamps_deduplicated.copy()
    parameters_dict["min_timestamp"] = min(timestamps_deduplicated)
    parameters_dict["max_timestamp"] = max(timestamps_deduplicated)

    interpolate_at_time_query = (
        "SELECT TagName, EventTime, Interpolated_Value as Value FROM "
        "(SELECT *, lag(EventTime) OVER (PARTITION BY TagName ORDER BY EventTime) AS Previous_EventTime, "
        "lag(Value) OVER (PARTITION BY TagName ORDER BY EventTime) AS Previous_Value, "
        "lead(EventTime) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_EventTime, "
        "lead(Value) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_Value, "
        "CASE WHEN Requested_EventTime = Found_EventTime THEN Value WHEN Next_EventTime IS NULL THEN Previous_Value WHEN Previous_EventTime IS NULL and Next_EventTime IS NULL THEN NULL ELSE Previous_Value + ((Next_Value - Previous_Value) * ((unix_timestamp(EventTime) - unix_timestamp(Previous_EventTime)) / (unix_timestamp(Next_EventTime) - unix_timestamp(Previous_EventTime)))) END AS Interpolated_Value FROM "
        "(SELECT coalesce(a.TagName, b.TagName) as TagName, coalesce(a.EventTime, b.EventTime) as EventTime, a.EventTime as Requested_EventTime, b.EventTime as Found_EventTime, b.Status, b.Value FROM "
        "(SELECT explode(array( "
        "{% for timestamp in timestamps -%} "
        "from_utc_timestamp(to_timestamp({{timestamp}}), {{time_zone}})"
        "{% if not loop.last %}"
        ", "
        "{% endif %}"
        "{% endfor %} "
        ")) AS EventTime, "
        "explode(array{{ tag_names | inclause }}) AS TagName) a FULL OUTER JOIN "
        "(SELECT * FROM (SELECT * FROM "
        "{{ business_unit | sqlsafe }}.sensors.{{ asset | sqlsafe }}_{{ data_security_level | sqlsafe }}_events_{{ data_type | sqlsafe }} "
        "WHERE EventDate BETWEEN "
        "{% if timestamps is defined %}"
        "date_sub(to_date(to_timestamp({{ min_timestamp }})), 1) AND date_add(to_date(to_timestamp({{ max_timestamp }})), 1)"
        "{% endif %}"
        "AND TagName in {{ tag_names | inclause }})) b ON a.EventTime = b.EventTime AND a.TagName = b.TagName))"
        "WHERE EventTime in ( "
        "{% for timestamp in timestamps -%} "
        "from_utc_timestamp(to_timestamp({{timestamp}}), {{time_zone}})"
        "{% if not loop.last %}"
        ", "
        "{% endif %}"
        "{% endfor %} "
        ")"       
    )
    
    interpolation_at_time_parameters = {
        "business_unit": parameters_dict['business_unit'].lower(),
        "region": parameters_dict['region'].lower(),
        "asset": parameters_dict['asset'].lower(),
        "data_security_level": parameters_dict['data_security_level'].lower(),
        "data_type": parameters_dict['data_type'].lower(),
        "tag_names": list(dict.fromkeys(parameters_dict['tag_names'])),
        "timestamps": parameters_dict['timestamps'],
        "time_zone": parameters_dict["time_zone"],
        "min_timestamp": parameters_dict["min_timestamp"],
        "max_timestamp": parameters_dict["max_timestamp"]
    }

    sql_template = JinjaSql(param_style='pyformat')
    query, bind_params = sql_template.prepare_query(interpolate_at_time_query, interpolation_at_time_parameters)
    sql_query = _get_sql_from_template(query, bind_params)
    return sql_query

def _metadata_query(parameters_dict: dict) -> str:
    
    metadata_query  = (
        "SELECT * FROM "
        "{{ business_unit | sqlsafe }}.sensors.{{ asset | sqlsafe }}_{{ data_security_level | sqlsafe }}_metadata "
        "{% if tag_names is defined and tag_names|length > 0 %}" + 
        "WHERE TagName in {{ tag_names | inclause }}"
        "{% endif %}"
    )

    metadata_parameters = {
        "business_unit": parameters_dict['business_unit'].lower(),
        "region": parameters_dict['region'].lower(),
        "asset": parameters_dict['asset'].lower(),
        "data_security_level": parameters_dict['data_security_level'].lower(),
        "tag_names": list(dict.fromkeys(parameters_dict['tag_names']))
    }

    sql_template = JinjaSql(param_style='pyformat')
    query, bind_params = sql_template.prepare_query(metadata_query, metadata_parameters)
    sql_query = _get_sql_from_template(query, bind_params)
    return sql_query    

def _time_weighted_average_query(parameters_dict: dict):

    time_weighted_average_query = (
        "SELECT TagName, EventTime, Value FROM "
        "(SELECT TagName, WindowEventTime as EventTime, Step, CASE WHEN Step == true THEN sum(step_values) / sum(minutes_diff) ELSE (0.5 * sum(weights) / sum(minutes_diff)) END as Value FROM "
        "(SELECT EventTime, WindowEventTime, TagName, {{ step }} as Step, Status, Value, lead(EventTime) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_EventTime, lead(Value) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_Value, "
        "((unix_timestamp(Next_EventTime) - unix_timestamp(EventTime)) / 60) as minutes_diff, (Value + Next_Value) as sum_values, (minutes_diff * sum_values) as weights, (Value * minutes_diff) as step_values FROM "
        "(SELECT TagName, EventTime, WindowEventTime, Status, last_value(Value, true) OVER (PARTITION BY TagName ORDER BY EventTime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Value "
        "FROM (SELECT coalesce(a.TagName, b.TagName) as TagName, coalesce(a.EventTime, b.EventTime) as EventTime, window(coalesce(a.EventTime, b.EventTime), '{{ window_size_mins }} minutes').start WindowEventTime, b.Status, b.Value "
        "FROM (SELECT explode(sequence(from_utc_timestamp(to_timestamp({{ start_date }}), {{ time_zone }}), from_utc_timestamp(to_timestamp({{ end_date }}), {{ time_zone }}), INTERVAL '{{ window_size_mins }} minute')) AS EventTime, explode(array{{ tag_names | inclause  }}) AS TagName) a FULL "
        "OUTER JOIN (SELECT * FROM (SELECT * FROM {{ business_unit | sqlsafe }}.sensors.{{ asset | sqlsafe }}_{{ data_security_level | sqlsafe }}_events_{{ data_type | sqlsafe }} "
        "WHERE EventDate BETWEEN date_sub(to_date(to_timestamp({{ start_date }})), 1) AND date_add(to_date(to_timestamp({{ end_date }})), 1) AND TagName in ({{ tag_names | inclause }}))) b ON a.EventTime = b.EventTime AND a.TagName = b.TagName))) "
        "WHERE Next_EventTime IS NOT NULL GROUP BY TagName, WindowEventTime, Step) "
        "WHERE EventTime BETWEEN from_utc_timestamp(to_timestamp({{ start_date }}), {{ time_zone }}) AND from_utc_timestamp(to_timestamp({{ end_date }}), {{ time_zone }}) "
    )

    time_weighted_average_parameters = {
        "business_unit": parameters_dict['business_unit'].lower(),
        "region": parameters_dict['region'].lower(),
        "asset": parameters_dict['asset'].lower(),
        "data_security_level": parameters_dict['data_security_level'].lower(),
        "data_type": parameters_dict['data_type'].lower(),
        "tag_names": list(dict.fromkeys(parameters_dict['tag_names'])),
        "start_date": parameters_dict['start_date'],
        "end_date": parameters_dict['end_date'],
        "window_size_mins": parameters_dict["window_size_mins"],
        #"window_length": parameters_dict["window_length"],
        #"include_bad_data": parameters_dict["include_bad_data"],
        "step": parameters_dict["step"].lower(),
        "time_zone": parameters_dict["time_zone"]      
    }

    sql_template = JinjaSql(param_style='pyformat')
    query, bind_params = sql_template.prepare_query(time_weighted_average_query, time_weighted_average_parameters)
    sql_query = _get_sql_from_template(query, bind_params)
    return sql_query

def _query_builder(parameters_dict: dict, metadata=False, raw=False, resample=False, interpolate=False, interpolation_at_time=False, time_weighted_average=False) -> str:
    if "tag_names" not in parameters_dict:
        parameters_dict["tag_names"] = []
    tagnames_deduplicated = list(dict.fromkeys(parameters_dict['tag_names'])) #remove potential duplicates in tags
    parameters_dict["tag_names"] = tagnames_deduplicated.copy()
    tag_name_string = ', '.join('"{0}"'.format(tagname) for tagname in tagnames_deduplicated)

    if metadata:
        return _metadata_query(parameters_dict)
    
    parameters_dict = _parse_dates(parameters_dict)
    
    if interpolation_at_time:
        return _interpolation_at_time(parameters_dict)

    if raw:
        return _raw_query(parameters_dict)

    if resample:
        sample_prepared_query, sample_query, sample_parameters = _sample_query(parameters_dict)

        if "interpolation_method" not in parameters_dict:
            return sample_prepared_query

    if interpolate:
        return _interpolation_query(parameters_dict, sample_query, sample_parameters)
    
    if time_weighted_average:
        return _time_weighted_average_query(parameters_dict)
    
    