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
from src.sdk.python.rtdip_sdk.queries.time_series import time_weighted_average
from src.api.v1.models import (
    BaseQueryParams,
    BaseHeaders,
    ResampleInterpolateResponse,
    PivotResponse,
    HTTPError,
    RawQueryParams,
    TagsQueryParams,
    TagsBodyParams,
    TimeWeightedAverageQueryParams,
    PivotQueryParams,
    LimitOffsetQueryParams,
)
from src.api.v1.common import common_api_setup_tasks, json_response, lookup_before_get


def time_weighted_average_events_get(
    base_query_parameters,
    raw_query_parameters,
    tag_query_parameters,
    time_weighted_average_parameters,
    pivot_parameters,
    limit_offset_parameters,
    base_headers,
):
    try:
        (connection, parameters) = common_api_setup_tasks(
            base_query_parameters,
            raw_query_parameters=raw_query_parameters,
            tag_query_parameters=tag_query_parameters,
            time_weighted_average_query_parameters=time_weighted_average_parameters,
            pivot_query_parameters=pivot_parameters,
            limit_offset_query_parameters=limit_offset_parameters,
            base_headers=base_headers,
        )

        if all(
            (key in parameters and parameters[key] != None)
            for key in ["business_unit", "asset", "data_security_level", "data_type"]
        ):
            # if have all required params, run normally
            data = time_weighted_average.get(connection, parameters)
        else:
            # else wrap in lookup function that finds tablenames and runs function (if mutliple tables, handles concurrent requests)
            data = lookup_before_get("time_weighted_average", connection, parameters)

        return json_response(data, limit_offset_parameters)
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))


get_description = """
## Time Weighted Average 

Time weighted average of raw timeseries data.
"""


@api_v1_router.get(
    path="/events/timeweightedaverage",
    name="Time Weighted Average GET",
    description=get_description,
    tags=["Events"],
    responses={
        200: {"model": Union[ResampleInterpolateResponse, PivotResponse]},
        400: {"model": HTTPError},
    },
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Time Weighted Averages Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/time-weighted-average/",
        }
    },
)
async def time_weighted_average_get(
    base_query_parameters: BaseQueryParams = Depends(),
    raw_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsQueryParams = Depends(),
    time_weighted_average_parameters: TimeWeightedAverageQueryParams = Depends(),
    pivot_parameters: PivotQueryParams = Depends(),
    limit_offset_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return time_weighted_average_events_get(
        base_query_parameters,
        raw_query_parameters,
        tag_query_parameters,
        time_weighted_average_parameters,
        pivot_parameters,
        limit_offset_parameters,
        base_headers,
    )


post_description = """
## Time Weighted Average 

Time weighted average of raw timeseries data via a POST method to enable providing a list of tag names that can exceed url length restrictions via GET Query Parameters.
"""


@api_v1_router.post(
    path="/events/timeweightedaverage",
    name="Time Weighted Average POST",
    description=get_description,
    tags=["Events"],
    responses={
        200: {"model": Union[ResampleInterpolateResponse, PivotResponse]},
        400: {"model": HTTPError},
    },
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Time Weighted Averages Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/time-weighted-average/",
        }
    },
)
async def time_weighted_average_post(
    base_query_parameters: BaseQueryParams = Depends(),
    raw_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsBodyParams = Body(default=...),
    time_weighted_average_parameters: TimeWeightedAverageQueryParams = Depends(),
    pivot_parameters: PivotQueryParams = Depends(),
    limit_offset_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return time_weighted_average_events_get(
        base_query_parameters,
        raw_query_parameters,
        tag_query_parameters,
        time_weighted_average_parameters,
        pivot_parameters,
        limit_offset_parameters,
        base_headers,
    )
