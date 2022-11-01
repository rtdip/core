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

def _fix_dates(parameters_dict):
    if len(parameters_dict['start_date']) == 10:
        parameters_dict['start_date'] = parameters_dict['start_date']+'T00:00:00'

    if len(parameters_dict['end_date']) == 10:
        parameters_dict['end_date'] = parameters_dict['end_date']+'T23:59:59'
    
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
        "SELECT EventTime, TagName, Status, Value FROM "
        "{{ business_unit | sqlsafe }}.sensors.{{ asset | sqlsafe }}_{{ data_security_level | sqlsafe }}_events_{{ data_type | sqlsafe }} "
        "WHERE EventDate BETWEEN to_date({{ start_date }}) AND to_date({{ end_date }}) AND EventTime BETWEEN to_timestamp({{ start_date }}) AND to_timestamp({{ end_date }}) AND TagName in {{ tag_names | inclause }} "
        "{% if include_bad_data is defined and include_bad_data == false %}"
        "AND Status = 'Good'"
        "{% endif %}"
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
        "include_bad_data": parameters_dict['include_bad_data']  
    }

    sql_template = JinjaSql(param_style='pyformat')
    query, bind_params = sql_template.prepare_query(raw_query, raw_parameters)
    sql_query = _get_sql_from_template(query, bind_params)
    return sql_query

def _sample_query(parameters_dict: dict) -> tuple:

    sample_query = (
        "SELECT DISTINCT TagName, w.start AS EventTime, {{ agg_method | sqlsafe }}(Value) OVER "
        "(PARTITION BY TagName, w.start ORDER BY EventTime ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING ) AS Value FROM ("
        "SELECT EventTime, WINDOW(EventTime, {{ sample_rate + ' ' + sample_unit }}) w, TagName, Status, Value FROM "
        "{{ business_unit | sqlsafe }}.sensors.{{ asset | sqlsafe }}_{{ data_security_level | sqlsafe }}_events_{{ data_type | sqlsafe }} "
        "WHERE EventDate BETWEEN to_date({{ start_date }}) AND to_date({{ end_date }}) AND EventTime BETWEEN to_timestamp({{ start_date }}) AND to_timestamp({{ end_date }}) AND TagName in {{ tag_names | inclause }} "
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
        "agg_method": parameters_dict['agg_method']
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
        "(SELECT explode(sequence(to_timestamp({{ start_date }}), to_timestamp({{ end_date }}), INTERVAL {{ sample_rate + ' ' + sample_unit }})) AS EventTime, explode(array({{ tag_name_string }})) AS TagName) a "
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

def _query_builder(parameters_dict: dict, metadata=False) -> str:
    if "tag_names" not in parameters_dict:
        parameters_dict["tag_names"] = []
    tagnames_deduplicated = list(dict.fromkeys(parameters_dict['tag_names'])) #remove potential duplicates in tags
    parameters_dict["tag_names"] = tagnames_deduplicated.copy()
    tag_name_string = ', '.join('"{0}"'.format(tagname) for tagname in tagnames_deduplicated)

    if metadata:
        return _metadata_query(parameters_dict)

    parameters_dict = _fix_dates(parameters_dict)

    if "agg_method" not in parameters_dict:
        return _raw_query(parameters_dict)

    if "sample_rate" in parameters_dict:
        sample_prepared_query, sample_query, sample_parameters = _sample_query(parameters_dict)

        if "interpolation_method" not in parameters_dict:
            return sample_prepared_query

    if "interpolation_method" in parameters_dict:
        return _interpolation_query(parameters_dict, sample_query, sample_parameters, tag_name_string)