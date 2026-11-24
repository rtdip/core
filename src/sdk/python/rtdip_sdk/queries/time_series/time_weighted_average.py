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
import json
import logging
import pandas as pd
from ._time_series_query_builder import _query_builder


def _build_twa_boundary_probe(parameters_dict, window_length):
    start_date = parameters_dict["start_date"]
    end_date = parameters_dict["end_date"]
    tagname_column = parameters_dict.get("tagname_column", "TagName")
    timestamp_column = parameters_dict.get("timestamp_column", "EventTime")
    status_column = parameters_dict.get("status_column") or "Status"
    value_column = parameters_dict.get("value_column", "Value")
    tag_names = list(dict.fromkeys(parameters_dict["tag_names"]))
    case_insensitive = parameters_dict.get("case_insensitivity_tag_search", False)

    if parameters_dict.get("source"):
        source_table = f"`{parameters_dict['source'].lower()}`"
    else:
        source_table = (
            f"`{parameters_dict['business_unit'].lower()}`.`sensors`.`"
            f"{parameters_dict['asset'].lower()}_{parameters_dict['data_security_level'].lower()}"
            f"_events_{parameters_dict['data_type'].lower()}`"
        )

    quoted_tags = "', '".join(tag.upper() if case_insensitive else tag for tag in tag_names)
    if case_insensitive:
        tag_filter = f"UPPER(`{tagname_column}`) IN ('{quoted_tags}')"
    else:
        tag_filter = f"`{tagname_column}` IN ('{quoted_tags}')"

    include_status = parameters_dict.get("status_column", "Status") is not None
    good_status = f"s.`{status_column}` <> 'Bad'" if include_status else "true"
    bad_filter = (
        f" AND `{status_column}` <> 'Bad'"
        if include_status and not parameters_dict.get("include_bad_data", False)
        else ""
    )

    step = parameters_dict["step"]
    requested_source = f"(SELECT explode(array('{quoted_tags}')) AS `{tagname_column}`) requested"
    step_column = "false"
    if step == "metadata":
        metadata_source = parameters_dict.get("metadata_source")
        if metadata_source:
            metadata_table = metadata_source.lower()
        else:
            metadata_table = (
                f"`{parameters_dict['business_unit'].lower()}`.`sensors`.`"
                f"{parameters_dict['asset'].lower()}_{parameters_dict['data_security_level'].lower()}_metadata`"
            )
        metadata_tagname_column = parameters_dict.get("metadata_tagname_column", "TagName")
        metadata_join = (
            f"UPPER(m.`{metadata_tagname_column}`) = UPPER(requested.`{tagname_column}`)"
            if case_insensitive
            else f"m.`{metadata_tagname_column}` = requested.`{tagname_column}`"
        )
        requested_source += f" LEFT JOIN {metadata_table} m ON {metadata_join}"
        step_column = "IFNULL(m.Step, false)"

    event_join = (
        f"UPPER(s.`{tagname_column}`) = UPPER(requested.`{tagname_column}`)"
        if case_insensitive
        else f"s.`{tagname_column}` = requested.`{tagname_column}`"
    )
    probe_query = (
        f"SELECT requested.`{tagname_column}` AS `{tagname_column}`, "
        f"MAX(CASE WHEN {step_column} OR (s.`{timestamp_column}` <= to_timestamp('{start_date}') AND {good_status} AND s.`{value_column}` IS NOT NULL) THEN 1 ELSE 0 END) AS HasPrevious, "
        f"MAX(CASE WHEN {step_column} OR (s.`{timestamp_column}` >= to_timestamp('{end_date}') AND {good_status} AND s.`{value_column}` IS NOT NULL) THEN 1 ELSE 0 END) AS HasNext "
        f"FROM {requested_source} LEFT JOIN (SELECT * FROM {source_table} WHERE {tag_filter} "
        f"AND `{timestamp_column}` >= timestampadd(day, -{window_length}, to_timestamp('{start_date}')) "
        f"AND `{timestamp_column}` <= timestampadd(day, {window_length}, to_timestamp('{end_date}')){bad_filter}) s "
        f"ON {event_join} GROUP BY requested.`{tagname_column}`"
    )
    if parameters_dict.get("to_json", False):
        return f"SELECT to_json(struct(*)) AS Value FROM ({probe_query}) probe"
    return probe_query


def _read_probe_status(result):
    if isinstance(result, pd.DataFrame):
        if result.empty or "HasPrevious" not in result.columns or "HasNext" not in result.columns:
            return True
        return not (
            pd.to_numeric(result["HasPrevious"], errors="coerce").fillna(0).gt(0).all()
            and pd.to_numeric(result["HasNext"], errors="coerce").fillna(0).gt(0).all()
        )

    if isinstance(result, dict) and result.get("data"):
        try:
            rows = json.loads(f"[{result['data']}]")
        except (TypeError, json.JSONDecodeError):
            return True
        if not rows:
            return True
        return not (
            all(row.get("HasPrevious") == 1 for row in rows)
            and all(row.get("HasNext") == 1 for row in rows)
        )

    return True


def get(connection: object, parameters_dict: dict) -> pd.DataFrame:
    """
    A function that receives a dataframe of raw tag data and performs a time weighted averages, returning the results.

    This function requires the input of a pandas dataframe acquired via the rtdip.functions.raw() method and the user to input a dictionary of parameters. (See Attributes table below)

    Pi data points will either have step enabled (True) or step disabled (False). You can specify whether you want step to be fetched by "Pi" or you can set the step parameter to True/False in the dictionary below.

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, or deprecated TURBODBC SQL Connect)
        parameters_dict (dict): A dictionary of parameters (see Attributes table below)

    Attributes:
        business_unit (str): Business unit
        region (str): Region
        asset (str): Asset
        data_security_level (str): Level of data security
        data_type (str): Type of the data (float, integer, double, string)
        tag_names (list): List of tagname or tagnames
        start_date (str): Start date (Either a utc date in the format YYYY-MM-DD or a utc datetime in the format YYYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        end_date (str): End date (Either a utc date in the format YYYY-MM-DD or a utc datetime in the format YYYY-MM-DDTHH:MM:SS or specify the timezone offset in the format YYYY-MM-DDTHH:MM:SS+zz:zz)
        time_interval_rate (str): The time interval rate (numeric input)
        time_interval_unit (str): The time interval unit (second, minute, day, hour)
        window_length (int): Fixed boundary-search window in days (0-366). When omitted, corrected linear modes expand through 7, 30, 180, and 366 days; legacy modes retain the 1-day window.
        include_bad_data (bool): Include "Bad" data points with True or remove "Bad" data points with False
        step (str): "true" uses legacy step integration; "false" uses corrected linear interpolation. Explicit "metadata" uses corrected interpolation for tags with false, missing, or null Step metadata. Omitting step retains legacy metadata behavior.
        display_uom (optional bool): Display the unit of measure with True or False. Does not apply to pivoted tables. Defaults to False
        pivot (bool): Pivot the data on timestamp column with True or do not pivot the data with False
        limit (optional int): The number of rows to be returned
        offset (optional int): The number of rows to skip before returning rows
        case_insensitivity_tag_search (optional bool): Search for tags using case insensitivity with True or case sensitivity with False

    Returns:
        DataFrame: A dataframe containing the time weighted averages.

    !!! warning
        Setting `case_insensitivity_tag_search` to True will result in a longer query time.

    !!! Note
        `display_uom` True will not work in conjunction with `pivot` set to True.
    """
    if isinstance(parameters_dict["tag_names"], list) is False:
        raise ValueError("tag_names must be a list")

    if "pivot" in parameters_dict and "display_uom" in parameters_dict:
        if parameters_dict["pivot"] is True and parameters_dict["display_uom"] is True:
            raise ValueError("pivot True and display_uom True cannot be used together")

    parameters_dict.setdefault("step_was_provided", "step" in parameters_dict)
    parameters_dict.setdefault(
        "window_length_was_provided", "window_length" in parameters_dict
    )
    parameters_dict.setdefault("window_length", 1)
    if not 0 <= parameters_dict["window_length"] <= 366:
        raise ValueError("window_length must be between 0 and 366 days")
    if "step" not in parameters_dict:  # default step to metadata if not provided
        parameters_dict["step"] = "metadata"

    step_was_provided = parameters_dict.get(
        "step_was_provided", "step" in parameters_dict
    )
    adaptive_linear_search = (
        parameters_dict["step"] == "false"
        or (parameters_dict["step"] == "metadata" and step_was_provided)
    ) and not parameters_dict["window_length_was_provided"]
    search_windows = [7, 30, 180, 366] if adaptive_linear_search else [
        parameters_dict["window_length"]
    ]

    try:
        try:
            df = None
            selected_window = search_windows[-1]
            if adaptive_linear_search:
                for window_length in search_windows:
                    probe = _build_twa_boundary_probe(parameters_dict, window_length)
                    cursor = connection.cursor()
                    try:
                        cursor.execute(probe)
                        probe_result = cursor.fetch_all()
                    finally:
                        cursor.close()

                    if not _read_probe_status(probe_result):
                        selected_window = window_length
                        break

            tag_names = list(dict.fromkeys(parameters_dict["tag_names"]))
            split_tags = (
                len(tag_names) > 1
                and not parameters_dict.get("pivot", False)
                and not parameters_dict.get("display_uom", False)
            )

            if split_tags:
                dataframes = []
                dict_results = []
                for tag_name in tag_names:
                    query_parameters = parameters_dict.copy()
                    query_parameters["tag_names"] = [tag_name]
                    query_parameters["adaptive_linear_search"] = False
                    query_parameters["_linear_window_length"] = selected_window
                    query_parameters["limit"] = None
                    query_parameters["offset"] = None
                    query = _query_builder(query_parameters, "time_weighted_average")
                    cursor = connection.cursor()
                    try:
                        cursor.execute(query)
                        result = cursor.fetch_all()
                    finally:
                        cursor.close()

                    if isinstance(result, pd.DataFrame):
                        dataframes.append(result)
                    elif isinstance(result, dict):
                        dict_results.append(result)

                if dataframes:
                    df = pd.concat(dataframes, ignore_index=True)
                    sort_columns = [
                        column for column in (
                            parameters_dict.get("tagname_column", "TagName"),
                            parameters_dict.get("timestamp_column", "EventTime"),
                        ) if column in df.columns
                    ]
                    if sort_columns:
                        df = df.sort_values(sort_columns, kind="stable")
                    offset = parameters_dict.get("offset") or 0
                    limit = parameters_dict.get("limit")
                    df = df.iloc[offset : offset + limit if limit is not None else None].reset_index(drop=True)
                elif dict_results:
                    combined_data = []
                    for result in dict_results:
                        if not result.get("data"):
                            continue
                        rows = json.loads(f"[{result['data']}]")
                        combined_data.extend(rows)
                    tagname_column = parameters_dict.get("tagname_column", "TagName")
                    timestamp_column = parameters_dict.get("timestamp_column", "EventTime")
                    combined_data.sort(key=lambda row: (row.get(tagname_column) or "", row.get(timestamp_column) or ""))
                    offset = parameters_dict.get("offset") or 0
                    limit = parameters_dict.get("limit")
                    combined_data = combined_data[offset : offset + limit if limit is not None else None]
                    df = {
                        "data": ",".join(
                            json.dumps(row, separators=(",", ":"))
                            for row in combined_data
                        ),
                        "sample_row": json.dumps(combined_data[0], separators=(",", ":")) if combined_data else None,
                        "count": len(combined_data),
                    }
            else:
                query_parameters = parameters_dict.copy()
                query_parameters["adaptive_linear_search"] = False
                query_parameters["_linear_window_length"] = selected_window
                query = _query_builder(query_parameters, "time_weighted_average")
                cursor = connection.cursor()
                try:
                    cursor.execute(query)
                    df = cursor.fetch_all()
                finally:
                    cursor.close()

            connection.close()
            return df
        except Exception as e:
            logging.exception("error returning dataframe")
            raise e

    except Exception as e:
        logging.exception("error with time weighted average function")
        raise e
