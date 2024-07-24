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
import json
import os
import pandas as pd
import importlib.util
from typing import Any
from fastapi import Response
import dateutil.parser
from pandas import DataFrame
import pyarrow as pa
from pandas.io.json import build_table_schema
from src.sdk.python.rtdip_sdk.connectors import (
    DatabricksSQLConnection,
    ConnectionReturnType,
)

if importlib.util.find_spec("turbodbc") != None:
    from src.sdk.python.rtdip_sdk.connectors import TURBODBCSQLConnection
from src.api.auth import azuread
from .models import BaseHeaders, FieldSchema, LimitOffsetQueryParams, PaginationRow


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
    data: dict, limit_offset_parameters: LimitOffsetQueryParams
) -> Response:
    schema_df = pd.DataFrame()
    if data["data"] is not None and data["data"] != "":
        json_str = data["data"][0 : data["data"].find("}") + 1]
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
