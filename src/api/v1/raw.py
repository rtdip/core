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
import numpy as np
from pandas import DataFrame
from fastapi import HTTPException, Depends, Body

from src.sdk.python.rtdip_sdk.queries.time_series import raw
from src.api.v1.models import (
    BaseHeaders,
    BaseQueryParams,
    RawResponse,
    RawQueryParams,
    TagsQueryParams,
    TagsBodyParams,
    LimitOffsetQueryParams,
    HTTPError,
)
from src.api.auth.azuread import oauth2_scheme
from src.api.v1.common import common_api_setup_tasks, json_response, lookup_before_get
from src.api.FastAPIApp import api_v1_router


def _mask_bad_data_values(data):
    if isinstance(data, DataFrame):
        masked_data = data.copy()
        masked_data["Value"] = masked_data["Value"].astype(object)
        masked_data.loc[masked_data["Status"] == "Bad", "Value"] = "Bad Data"
        return masked_data

    masked_data = data.copy()
    for key in ("data", "sample_row"):
        serialized_rows = masked_data.get(key)
        if not serialized_rows:
            continue

        rows = json.loads(f"[{serialized_rows}]")
        for row in rows:
            if row.get("Status") == "Bad":
                row["Value"] = "Bad Data"
        masked_data[key] = ",".join(
            json.dumps(row, separators=(",", ":")) for row in rows
        )

    return masked_data


def raw_events_get(
    base_query_parameters,
    raw_query_parameters,
    tag_query_parameters,
    limit_offset_parameters,
    base_headers,
    mask_bad_data=False,
):
    try:
        connection, parameters = common_api_setup_tasks(
            base_query_parameters,
            raw_query_parameters=raw_query_parameters,
            tag_query_parameters=tag_query_parameters,
            limit_offset_query_parameters=limit_offset_parameters,
            base_headers=base_headers,
        )

        if all(
            (key in parameters and parameters[key] != None)
            for key in ["business_unit", "asset", "data_security_level", "data_type"]
        ):
            # if have all required params, run normally
            data = raw.get(connection, parameters)
        else:
            # else wrap in lookup function that finds tablenames and runs function (if mutliple tables, handles concurrent requests)
            data = lookup_before_get("raw", connection, parameters)

        if mask_bad_data and raw_query_parameters.include_bad_data:
            data = _mask_bad_data_values(data)

        return json_response(data, limit_offset_parameters)
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))


get_description = """
## Raw 

Retrieval of raw timeseries data.
"""


@api_v1_router.get(
    path="/events/raw",
    name="Raw GET",
    description=get_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": RawResponse}, 400: {"model": HTTPError}},
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Raw Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/raw/",
        }
    },
)
async def raw_get(
    base_query_parameters: BaseQueryParams = Depends(),
    raw_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsQueryParams = Depends(),
    limit_offset_query_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return raw_events_get(
        base_query_parameters,
        raw_query_parameters,
        tag_query_parameters,
        limit_offset_query_parameters,
        base_headers,
    )


post_description = """
## Raw 

Retrieval of raw timeseries data via a POST method to enable providing a list of tag names that can exceed url length restrictions via GET Query Parameters.
"""


@api_v1_router.post(
    path="/events/raw",
    name="Raw POST",
    description=post_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": RawResponse}, 400: {"model": HTTPError}},
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Raw Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/raw/",
        }
    },
)
async def raw_post(
    base_query_parameters: BaseQueryParams = Depends(),
    raw_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsBodyParams = Body(default=...),
    limit_offset_query_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return raw_events_get(
        base_query_parameters,
        raw_query_parameters,
        tag_query_parameters,
        limit_offset_query_parameters,
        base_headers,
    )


masked_get_description = """
## Raw with Bad Data Masked

Retrieval of raw timeseries data. When Bad data is included, its Value is returned as "Bad Data".
"""


@api_v1_router.get(
    path="/events/rawmasked",
    name="Raw Masked GET",
    description=masked_get_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": RawResponse}, 400: {"model": HTTPError}},
)
async def raw_masked_get(
    base_query_parameters: BaseQueryParams = Depends(),
    raw_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsQueryParams = Depends(),
    limit_offset_query_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return raw_events_get(
        base_query_parameters,
        raw_query_parameters,
        tag_query_parameters,
        limit_offset_query_parameters,
        base_headers,
        mask_bad_data=True,
    )


masked_post_description = """
## Raw with Bad Data Masked

Retrieval of raw timeseries data with a POST body for tag names. When Bad data is included, its Value is returned as "Bad Data".
"""


@api_v1_router.post(
    path="/events/rawmasked",
    name="Raw Masked POST",
    description=masked_post_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": RawResponse}, 400: {"model": HTTPError}},
)
async def raw_masked_post(
    base_query_parameters: BaseQueryParams = Depends(),
    raw_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsBodyParams = Body(default=...),
    limit_offset_query_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return raw_events_get(
        base_query_parameters,
        raw_query_parameters,
        tag_query_parameters,
        limit_offset_query_parameters,
        base_headers,
        mask_bad_data=True,
    )
