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


import logging
from typing import Union
from src.api.FastAPIApp import api_v1_router
from fastapi import HTTPException, Depends, Body

from src.sdk.python.rtdip_sdk.queries.time_series import plot
from src.api.v1.models import (
    BaseQueryParams,
    BaseHeaders,
    ResampleInterpolateResponse,
    HTTPError,
    RawQueryParams,
    TagsQueryParams,
    TagsBodyParams,
    PlotQueryParams,
    PivotQueryParams,
    LimitOffsetQueryParams,
)
from src.api.v1.common import common_api_setup_tasks, json_response, lookup_before_get


def plot_events_get(
    base_query_parameters,
    raw_query_parameters,
    tag_query_parameters,
    plot_query_parameters,
    limit_offset_parameters,
    base_headers,
):
    try:
        (connection, parameters) = common_api_setup_tasks(
            base_query_parameters,
            raw_query_parameters=raw_query_parameters,
            tag_query_parameters=tag_query_parameters,
            plot_query_parameters=plot_query_parameters,
            limit_offset_query_parameters=limit_offset_parameters,
            base_headers=base_headers,
        )

        if all(
            (key in parameters and parameters[key] != None)
            for key in ["business_unit", "asset", "data_security_level", "data_type"]
        ):
            # if have all required params, run normally
            data = plot.get(connection, parameters)
        else:
            # else wrap in lookup function that finds tablenames and runs function (if mutliple tables, handles concurrent requests)
            data = lookup_before_get("plot", connection, parameters)

        return json_response(data, limit_offset_parameters)
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))


get_description = """
## Plot 

Plotting of resampled raw timeseries data and aggregated to Min, Max, First and Last and an Exception Value(Status = Bad) if it exists.
"""


@api_v1_router.get(
    path="/events/plot",
    name="Plot GET",
    description=get_description,
    tags=["Events"],
    responses={
        200: {"model": ResampleInterpolateResponse},
        400: {"model": HTTPError},
    },
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Plot Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/plot/",
        }
    },
)
async def plot_get(
    base_query_parameters: BaseQueryParams = Depends(),
    raw_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsQueryParams = Depends(),
    plot_query_parameters: PlotQueryParams = Depends(),
    limit_offset_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return plot_events_get(
        base_query_parameters,
        raw_query_parameters,
        tag_query_parameters,
        plot_query_parameters,
        limit_offset_parameters,
        base_headers,
    )


post_description = """
## Plot 

Plotting of resampled raw timeseries data and aggregated to Average, Min, Max, First and Last and an Exception Value(Status = Bad) if it exists, via a POST method to enable providing a list of tag names that can exceed url length restrictions via GET Query Parameters.
"""


@api_v1_router.post(
    path="/events/plot",
    name="Plot POST",
    description=post_description,
    tags=["Events"],
    responses={
        200: {"model": ResampleInterpolateResponse},
        400: {"model": HTTPError},
    },
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Resample Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/plot/",
        }
    },
)
async def plot_post(
    base_query_parameters: BaseQueryParams = Depends(),
    raw_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsBodyParams = Body(default=...),
    plot_query_parameters: PlotQueryParams = Depends(),
    limit_offset_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return plot_events_get(
        base_query_parameters,
        raw_query_parameters,
        tag_query_parameters,
        plot_query_parameters,
        limit_offset_parameters,
        base_headers,
    )
