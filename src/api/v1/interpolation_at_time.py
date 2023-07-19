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
import numpy as np
from src.api.FastAPIApp import api_v1_router
from fastapi import HTTPException, Depends, Body
import nest_asyncio
from pandas.io.json import build_table_schema
from src.sdk.python.rtdip_sdk.queries import interpolation_at_time
from src.api.v1.models import BaseQueryParams, ResampleInterpolateResponse, HTTPError, TagsQueryParams, TagsBodyParams, InterpolationAtTimeQueryParams
import src.api.v1.common

nest_asyncio.apply()

def interpolation_at_time_events_get(base_query_parameters, tag_query_parameters, interpolation_at_time_query_parameters):
    try:
        (connection, parameters) = src.api.v1.common.common_api_setup_tasks(
            base_query_parameters, 
            tag_query_parameters=tag_query_parameters,
            interpolation_at_time_query_parameters=interpolation_at_time_query_parameters
        )

        data = interpolation_at_time.get(connection, parameters)
        return ResampleInterpolateResponse(schema=build_table_schema(data, index=False, primary_key=False), data=data.replace({np.nan: None}).to_dict(orient="records"))
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))

get_description = """
## Interpolation at Time

Interpolation at Time of raw timeseries data. Refer to the following [documentation](https://www.rtdip.io/sdk/code-reference/query/interpolation_at_time/) for further information.
"""

@api_v1_router.get(
    path="/events/interpolationattime",
    name="Interpolation at Time GET",
    description=get_description, 
    tags=["Events"], 
    responses={200: {"model": ResampleInterpolateResponse}, 400: {"model": HTTPError}}
)
async def interpolate_get(
        base_query_parameters: BaseQueryParams = Depends(), 
        tag_query_parameters: TagsQueryParams = Depends(),
        interpolation_at_time_query_parameters: InterpolationAtTimeQueryParams = Depends()
    ):
    return interpolation_at_time_events_get(base_query_parameters, tag_query_parameters, interpolation_at_time_query_parameters)

post_description = """
## Interpolation at Time

Interpolation at time of raw timeseries data via a POST method to enable providing a list of tag names that can exceed url length restrictions via GET Query Parameters. Refer to the following [documentation](https://www.rtdip.io/sdk/code-reference/query/interpolation_at_time/) for further information.
"""

@api_v1_router.post(
    path="/events/interpolationattime",
    name="Interpolation at Time POST",
    description=get_description, 
    tags=["Events"], 
    responses={200: {"model": ResampleInterpolateResponse}, 400: {"model": HTTPError}}
)
async def interpolate_post(
        base_query_parameters: BaseQueryParams = Depends(), 
        tag_query_parameters: TagsBodyParams = Body(default=...),
        interpolation_at_time_query_parameters: InterpolationAtTimeQueryParams = Depends()
    ):
    return interpolation_at_time_events_get(base_query_parameters, tag_query_parameters, interpolation_at_time_query_parameters)    