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
from datetime import datetime

def _is_date_format(dt, format):
    try:
        return datetime.strptime(dt , format)
    except:
        return False

def _fix_date(dt, is_end_date = False):
    if _is_date_format(dt, "%Y-%m-%d"):
        _time = "T23:59:59" if is_end_date == True else "T00:00:00"
        return dt + _time + "+00:00"
    elif _is_date_format(dt, "%Y-%m-%dT%H:%M:%S"):
        return dt + "+00:00"
    elif _is_date_format(dt, "%Y-%m-%dT%H:%M:%S%z"):
        return dt
    else: 
        raise ValueError(f"Inputted datetime: '{dt}', is not in the correct format")
        
def _fix_dates(parameters_dict):
        
    parameters_dict["start_date"] = _fix_date(parameters_dict["start_date"])
    parameters_dict["end_date"] = _fix_date(parameters_dict["end_date"], True)

    parameters_dict["time_zone"] = datetime.strptime(parameters_dict["start_date"], "%Y-%m-%dT%H:%M:%S%z").strftime("%z")
    
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

def _interpolation_query(parameters_dict: dict, sample_query: str, sample_parameters: dict, tag_name_string: str) -> str:

    if parameters_dict["interpolation_method"] == "forward_fill":
        interpolation_method = 'last_value/UNBOUNDED PRECEDING/CURRENT ROW'

    if parameters_dict["interpolation_method"] == "backward_fill":
        interpolation_method = 'first_value/CURRENT ROW/UNBOUNDED FOLLOWING'

    interpolation_options = interpolation_method.split('/')

    interpolate_query = (
        "SELECT a.EventTime, a.TagName, {{ interpolation_options_0 | sqlsafe }}(b.Value, true) OVER (PARTITION BY a.TagName ORDER BY a.EventTime ROWS BETWEEN {{ interpolation_options_1 | sqlsafe }} AND {{ interpolation_options_2 | sqlsafe }}) AS Value FROM "
        "(SELECT explode(sequence(from_utc_timestamp(to_timestamp({{ start_date }}), \"{{ time_zone | sqlsafe }}\"), from_utc_timestamp(to_timestamp({{ end_date }}), \"{{ time_zone | sqlsafe }}\"), INTERVAL {{ sample_rate + ' ' + sample_unit }})) AS EventTime, "
        f"explode(array({tag_name_string })) AS TagName) a "
        f"LEFT OUTER JOIN ({sample_query}) b "
        "ON a.EventTime = b.EventTime "
        "AND a.TagName = b.TagName"        
    )
    
    interpolate_parameters = sample_parameters.copy()
    interpolate_parameters["interpolation_options_0"] = interpolation_options[0]
    interpolate_parameters["interpolation_options_1"] = interpolation_options[1]
    interpolate_parameters["interpolation_options_2"] = interpolation_options[2]
    interpolate_parameters["tag_name_string"] = tag_name_string

    sql_template = JinjaSql(param_style='pyformat')
    query, bind_params = sql_template.prepare_query(interpolate_query, interpolate_parameters)
    sql_query = _get_sql_from_template(query, bind_params)
    return sql_query

def _interpolation_at_time(parameters_dict: dict, tag_name_string: str) -> str:

    interpolate_at_time_query = (
        "SELECT TagName, EventTime, Interpolated_Value as Value FROM "
        "(SELECT *, lag(EventTime) OVER (PARTITION BY TagName ORDER BY EventTime) AS Previous_EventTime, "
        "lag(Value) OVER (PARTITION BY TagName ORDER BY EventTime) AS Previous_Value, "
        "lead(EventTime) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_EventTime, "
        "lead(Value) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_Value, "
        "CASE WHEN Next_EventTime IS NULL THEN Previous_Value WHEN Previous_EventTime IS NULL and Next_EventTime IS NULL THEN NULL ELSE (Next_Value - Previous_Value) * ((unix_timestamp(EventTime) - unix_timestamp(Previous_EventTime)) / (unix_timestamp(Next_EventTime) - unix_timestamp(Previous_EventTime))) END AS Interpolated_Value FROM "
        "(SELECT coalesce(a.TagName, b.TagName) as TagName, coalesce(a.EventTime, b.EventTime) as EventTime, b.Status, b.Value FROM "
        "(SELECT explode(array(from_utc_timestamp(to_timestamp({{ start_date }}), \"{{ time_zone | sqlsafe }}\"), from_utc_timestamp(to_timestamp({{ end_date }}), \"{{ time_zone | sqlsafe }}\"))) AS EventTime, "
        f"explode(array({tag_name_string})) AS TagName) a FULL OUTER JOIN "
        "(SELECT * FROM (SELECT * FROM "
        #ssip.time_series "
        "{{ business_unit | sqlsafe }}.sensors.{{ asset | sqlsafe }}_{{ data_security_level | sqlsafe }}_events_{{ data_type | sqlsafe }} "
        "WHERE EventDate BETWEEN date_sub(to_date(to_timestamp({{ start_date }})), 1) AND date_add(to_date(to_timestamp({{end_date}})), 1) AND TagName in {{ tag_names | inclause }})) b ON a.EventTime = b.EventTime AND a.TagName = b.TagName))"
        "WHERE EventTime in (from_utc_timestamp(to_timestamp({{ start_date }}), \"{{ time_zone | sqlsafe }}\"), from_utc_timestamp(to_timestamp({{ end_date }}), \"{{ time_zone | sqlsafe }}\"))"       
    )
    
    interpolation_at_time_parameters = {
        "business_unit": parameters_dict['business_unit'].lower(),
        "region": parameters_dict['region'].lower(),
        "asset": parameters_dict['asset'].lower(),
        "data_security_level": parameters_dict['data_security_level'].lower(),
        "data_type": parameters_dict['data_type'].lower(),
        "tag_names": list(dict.fromkeys(parameters_dict['tag_names'])),
        "start_date": parameters_dict['start_date'],
        "end_date": parameters_dict['end_date'],
        # "include_bad_data": parameters_dict['include_bad_data'],
        "time_zone": parameters_dict["time_zone"]
    }
    #interpolation_at_time_parameters["tag_name_string"] = tag_name_string

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

def _query_builder(parameters_dict: dict, metadata=False, interpolation_at_time=False) -> str:
    if "tag_names" not in parameters_dict:
        parameters_dict["tag_names"] = []
    tagnames_deduplicated = list(dict.fromkeys(parameters_dict['tag_names'])) #remove potential duplicates in tags
    parameters_dict["tag_names"] = tagnames_deduplicated.copy()
    tag_name_string = ', '.join('"{0}"'.format(tagname) for tagname in tagnames_deduplicated)

    if metadata:
        return _metadata_query(parameters_dict)

    parameters_dict = _fix_dates(parameters_dict)

    if interpolation_at_time:
        return _interpolation_at_time(parameters_dict, tag_name_string)

    if "agg_method" not in parameters_dict:
        return _raw_query(parameters_dict)

    if "sample_rate" in parameters_dict:
        sample_prepared_query, sample_query, sample_parameters = _sample_query(parameters_dict)

        if "interpolation_method" not in parameters_dict:
            return sample_prepared_query

    if "interpolation_method" in parameters_dict:
        return _interpolation_query(parameters_dict, sample_query, sample_parameters, tag_name_string)
    
    