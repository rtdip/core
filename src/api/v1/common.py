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

from datetime import datetime
import datetime as dt
import json
import os
import importlib.util

from typing import Any, List, Dict, Union
import requests
import json
import pandas as pd
import numpy as np

from fastapi import Response
from fastapi.responses import JSONResponse
import dateutil.parser

from pandas import DataFrame
import pyarrow as pa
from pandas.io.json import build_table_schema

from src.sdk.python.rtdip_sdk.connectors import (
    DatabricksSQLConnection,
    ConnectionReturnType,
)

from src.sdk.python.rtdip_sdk.queries.time_series import batch


if importlib.util.find_spec("turbodbc") != None:
    from src.sdk.python.rtdip_sdk.connectors import TURBODBCSQLConnection
from src.api.auth import azuread
from .models import BaseHeaders, FieldSchema, LimitOffsetQueryParams, PaginationRow
from decimal import Decimal


def common_api_setup_tasks(  # NOSONAR
    base_query_parameters,  # NOSONAR
    base_headers: BaseHeaders,  # NOSONAR
    metadata_query_parameters=None,
    raw_query_parameters=None,
    sql_query_parameters=None,
    tag_query_parameters=None,
    resample_query_parameters=None,
    plot_query_parameters=None,
    interpolate_query_parameters=None,
    interpolation_at_time_query_parameters=None,
    time_weighted_average_query_parameters=None,
    circular_average_query_parameters=None,
    circular_standard_deviation_query_parameters=None,
    summary_query_parameters=None,
    pivot_query_parameters=None,
    limit_offset_query_parameters=None,
):
    token = azuread.get_azure_ad_token(base_query_parameters.authorization)

    odbc_connection = os.getenv("RTDIP_ODBC_CONNECTION", "")

    databricks_server_host_name = (
        os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME")
        if base_headers.x_databricks_server_hostname is None
        else base_headers.x_databricks_server_hostname
    )

    databricks_http_path = (
        os.getenv("DATABRICKS_SQL_HTTP_PATH")
        if base_headers.x_databricks_http_path is None
        else base_headers.x_databricks_http_path
    )

    if odbc_connection == "turbodbc":
        connection = TURBODBCSQLConnection(
            databricks_server_host_name,
            databricks_http_path,
            token,
            ConnectionReturnType.String,
        )
    else:
        connection = DatabricksSQLConnection(
            databricks_server_host_name,
            databricks_http_path,
            token,
            ConnectionReturnType.String,
        )

    parameters = base_query_parameters.__dict__
    parameters["to_json"] = True

    if metadata_query_parameters != None:
        parameters = dict(parameters, **metadata_query_parameters.__dict__)
        if "tag_name" in parameters:
            if parameters["tag_name"] is None:
                parameters["tag_names"] = []
                parameters.pop("tag_name")
            else:
                parameters["tag_names"] = parameters.pop("tag_name")

    if raw_query_parameters != None:
        parameters = dict(parameters, **raw_query_parameters.__dict__)
        parameters["start_date"] = raw_query_parameters.start_date
        parameters["end_date"] = raw_query_parameters.end_date

    if sql_query_parameters != None:
        parameters = dict(parameters, **sql_query_parameters.__dict__)

    if tag_query_parameters != None:
        parameters = dict(parameters, **tag_query_parameters.__dict__)
        parameters["tag_names"] = parameters.pop("tag_name")

    if resample_query_parameters != None:
        parameters = dict(parameters, **resample_query_parameters.__dict__)

    if plot_query_parameters != None:
        parameters = dict(parameters, **plot_query_parameters.__dict__)

    if interpolate_query_parameters != None:
        parameters = dict(parameters, **interpolate_query_parameters.__dict__)

    if interpolation_at_time_query_parameters != None:
        parameters = dict(parameters, **interpolation_at_time_query_parameters.__dict__)

    if time_weighted_average_query_parameters != None:
        parameters = dict(parameters, **time_weighted_average_query_parameters.__dict__)

    if circular_average_query_parameters != None:
        parameters = dict(parameters, **circular_average_query_parameters.__dict__)

    if circular_standard_deviation_query_parameters != None:
        parameters = dict(
            parameters, **circular_standard_deviation_query_parameters.__dict__
        )

    if summary_query_parameters != None:
        parameters = dict(parameters, **summary_query_parameters.__dict__)

    if pivot_query_parameters != None:
        parameters = dict(parameters, **pivot_query_parameters.__dict__)

    if limit_offset_query_parameters != None:
        parameters = dict(parameters, **limit_offset_query_parameters.__dict__)

    return connection, parameters


def pagination(limit_offset_parameters: LimitOffsetQueryParams, rows: int):
    pagination = PaginationRow(
        limit=None,
        offset=None,
        next=None,
    )

    if (
        limit_offset_parameters.limit is not None
        or limit_offset_parameters.offset is not None
    ):
        next_offset = None

        if (
            rows == limit_offset_parameters.limit
            and limit_offset_parameters.offset is not None
        ):
            next_offset = limit_offset_parameters.offset + limit_offset_parameters.limit

        pagination = PaginationRow(
            limit=limit_offset_parameters.limit,
            offset=limit_offset_parameters.offset,
            next=next_offset,
        )

    return pagination


def datetime_parser(json_dict):
    for key, value in json_dict.items():
        try:
            json_dict[key] = (
                dateutil.parser.parse(value, ignoretz=True)
                if isinstance(value, str) and "eventtime" in key.lower()
                else value
            )
        except Exception:
            pass
    return json_dict


def json_response(
    data: Union[dict, DataFrame], limit_offset_parameters: LimitOffsetQueryParams
) -> Response:
    if isinstance(data, DataFrame):
        return Response(
            content="{"
            + '"schema":{},"data":{},"pagination":{}'.format(
                FieldSchema.model_validate(
                    build_table_schema(data, index=False, primary_key=False),
                ).model_dump_json(),
                data.replace({np.nan: None}).to_json(
                    orient="records", date_format="iso", date_unit="ns"
                ),
                pagination(limit_offset_parameters, data).model_dump_json(),
            )
            + "}",
            media_type="application/json",
        )
    else:
        schema_df = pd.DataFrame()
        if data["data"] is not None and data["data"] != "":
            json_str = data["sample_row"]
            json_dict = json.loads(json_str, object_hook=datetime_parser)
            schema_df = pd.json_normalize(json_dict)

        return Response(
            content="{"
            + '"schema":{},"data":{},"pagination":{}'.format(
                FieldSchema.model_validate(
                    build_table_schema(schema_df, index=False, primary_key=False),
                ).model_dump_json(),
                "[" + data["data"] + "]",
                pagination(limit_offset_parameters, data["count"]).model_dump_json(),
            )
            + "}",
            media_type="application/json",
        )


def json_response_batch(data_list: List[DataFrame]) -> Response:
    # Function to parse dataframe into dictionary along with schema
    def get_as_dict(data):
        def convert_value(x):
            if isinstance(x, pd.Timestamp):
                return x.isoformat(timespec="nanoseconds")
            elif isinstance(x, dt.date):
                return x.isoformat()
            elif isinstance(x, pd.Timedelta):
                return x.isoformat()
            elif isinstance(x, Decimal):
                return float(x)
            return x

        data_parsed = data.applymap(convert_value).replace({np.nan: None})
        schema = build_table_schema(data_parsed, index=False, primary_key=False)
        data_dict = data_parsed.to_dict(orient="records")

        return {"schema": schema, "data": data_dict}

    # Parse each dataframe into a dictionary containing the schema and the data as dict
    dict_content = {"data": [get_as_dict(data) for data in data_list]}

    return JSONResponse(content=dict_content)


def lookup_before_get(
    func_name: str, connection: DatabricksSQLConnection, parameters: Dict
):
    # Ensure returns data as DataFrames
    parameters["to_json"] = False

    # query mapping endpoint for tablenames - returns tags as array under each table key
    tag_table_mapping = query_mapping_endpoint(
        tags=parameters["tag_names"],
        mapping_endpoint=os.getenv("DATABRICKS_SERVING_ENDPOINT"),
        connection=connection,
    )

    # create list of parameter dicts for each table
    request_list = []
    for table in tag_table_mapping:
        params = parameters.copy()
        params["tag_names"] = tag_table_mapping[table]
        params.update(
            split_table_name(table)
        )  # Adds business_unit, asset, data_security_level, data_type
        request = {"type": func_name, "parameters_dict": params}
        request_list.append(request)

    # make default workers 3 as within one query typically will request from only a few tables at once
    max_workers = os.environ.get("LOOKUP_THREADPOOL_WORKERS", 3)

    # ensure max_workers is an integer
    max_workers = int(max_workers)

    # run function with each parameters concurrently
    results = batch.get(connection, request_list, threadpool_max_workers=max_workers)

    # Check if pivot is required
    should_pivot = parameters["pivot"] if "pivot" in parameters else False

    # Append/concat results as required
    data = concatenate_dfs_and_order(
        dfs_arr=results, pivot=should_pivot, tags=parameters["tag_names"]
    )

    return data


def query_mapping_endpoint(tags: list, mapping_endpoint: str, connection: Dict):
    # Form header dict with token from connection
    token = swap_for_databricks_token(connection.access_token)
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # Create body of request
    data = {"dataframe_records": [{"TagName": tag} for tag in tags]}
    data_json = json.dumps(data, allow_nan=True)

    # Make request to mapping endpoint
    response = requests.post(headers=headers, url=mapping_endpoint, data=data_json)
    if response.status_code != 200:
        raise Exception(
            f"Request failed with status {response.status_code}, {response.text}"
        )
    result = response.json()

    # Map tags to tables, where all tags belonging to each table are stored in an array
    tag_table_mapping = {}
    for row in result["outputs"]:
        # Check results are returned
        if any(row[x] == None for x in ["CatalogName", "SchemaName", "DataTable"]):
            raise Exception(
                f"One or more tags do not have tables associated with them, the data belongs to a confidential table, or you do not have access. If the tag belongs to a confidential table and you do have access, please supply the business_unit, asset, data_security_level and data_type"
            )

        # Construct full tablename from output
        table_name = f"""{row["CatalogName"]}.{row["SchemaName"]}.{row["DataTable"]}"""

        # Store table names along with tags in dict (all tags that share table under same key)
        if table_name not in tag_table_mapping:
            tag_table_mapping[table_name] = []

        tag_table_mapping[table_name].append(row["TagName"])

    return tag_table_mapping


def split_table_name(str):
    try:
        # Retireve parts by splitting string
        parts = str.split(".")
        business_unit = parts[0]
        schema = parts[1]
        asset_security_type = parts[2].split("_")

        # check if of correct format
        if schema != "sensors" and ("events" not in str or "metadata" not in str):
            raise Exception()

        # Get the asset, data security level and type
        asset = asset_security_type[0].lower()
        data_security_level = asset_security_type[1].lower()
        data_type = asset_security_type[
            len(asset_security_type) - 1
        ].lower()  # i.e. the final part

        #  Return the formatted object
        return {
            "business_unit": business_unit,
            "asset": asset,
            "data_security_level": data_security_level,
            "data_type": data_type,
        }
    except Exception as e:
        raise Exception(
            "Unsupported table name format supplied. Please use the format 'businessunit.schema.asset.datasecurityevel_events_datatype"
        )


def concatenate_dfs_and_order(dfs_arr: List[DataFrame], pivot: bool, tags: list):
    if pivot:
        # If pivoted, then must add columns horizontally
        concat_df = pd.concat(dfs_arr, axis=1, ignore_index=False)
        concat_df = concat_df.loc[:, ~concat_df.columns.duplicated()]

        # reorder columns so that they match the order of the tags provided
        time_col = concat_df.columns.to_list()[0]
        cols = [time_col, *tags]
        concat_df = concat_df[cols]

    else:
        # Otherwise, can concat vertically
        concat_df = pd.concat(dfs_arr, axis=0, ignore_index=True)

    return concat_df


def swap_for_databricks_token(azure_ad_token):
    DATABRICKS_SQL_SERVER_HOSTNAME = os.getenv("DATABRICKS_SQL_SERVER_HOSTNAME")

    token_response = requests.post(
        f"https://{DATABRICKS_SQL_SERVER_HOSTNAME}/api/2.0/token/create",
        headers={"Authorization": f"Bearer {azure_ad_token}"},
        json={"comment": "tag mapping token", "lifetime_seconds": 360},
    )

    if token_response.status_code == 200:
        DATABRICKS_TOKEN = token_response.json().get("token_value")
    else:
        DATABRICKS_TOKEN = ""

    return DATABRICKS_TOKEN
