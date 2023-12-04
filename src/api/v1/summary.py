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
from pandas.io.json import build_table_schema
from fastapi import Query, HTTPException, Depends, Body
import nest_asyncio
from src.sdk.python.rtdip_sdk.queries.time_series import summary
from src.api.v1.models import (
    BaseHeaders,
    BaseQueryParams,
    SummaryResponse,
    RawQueryParams,
    TagsQueryParams,
    TagsBodyParams,
    LimitOffsetQueryParams,
    HTTPError,
)
from src.api.auth.azuread import oauth2_scheme
from src.api.FastAPIApp import api_v1_router
import src.api.v1.common

nest_asyncio.apply()


def summary_events_get(
    base_query_parameters,
    summary_query_parameters,
    tag_query_parameters,
    limit_offset_parameters,
    base_headers,
):
    try:
        (connection, parameters) = src.api.v1.common.common_api_setup_tasks(
            base_query_parameters,
            summary_query_parameters=summary_query_parameters,
            tag_query_parameters=tag_query_parameters,
            limit_offset_query_parameters=limit_offset_parameters,
            base_headers=base_headers,
        )

        data = summary.get(connection, parameters)
        return SummaryResponse(
            schema=build_table_schema(data, index=False, primary_key=False),
            data=data.replace({np.nan: None}).to_dict(orient="records"),
        )
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))


get_description = """
## Summary Statistics

Retrieval of summary statistics of timeseries data.
"""


@api_v1_router.get(
    path="/events/summary",
    name="Summary GET",
    description=get_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": SummaryResponse}, 400: {"model": HTTPError}},
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Summary Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/summary/",
        }
    },
)
async def summary_get(
    base_query_parameters: BaseQueryParams = Depends(),
    summary_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsQueryParams = Depends(),
    limit_offset_query_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return summary_events_get(
        base_query_parameters,
        summary_query_parameters,
        tag_query_parameters,
        limit_offset_query_parameters,
        base_headers,
    )


post_description = """
## Summary Statistics

Retrieval of summary statistics of timeseries data via a POST method to enable providing a list of tag names that can exceed url length restrictions via GET Query Parameters.
"""


@api_v1_router.post(
    path="/events/summary",
    name="Summary POST",
    description=post_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": SummaryResponse}, 400: {"model": HTTPError}},
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Summary Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/summary/",
        }
    },
)
async def summary_post(
    base_query_parameters: BaseQueryParams = Depends(),
    summary_query_parameters: RawQueryParams = Depends(),
    tag_query_parameters: TagsBodyParams = Body(default=...),
    limit_offset_query_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return summary_events_get(
        base_query_parameters,
        summary_query_parameters,
        tag_query_parameters,
        limit_offset_query_parameters,
        base_headers,
    )
