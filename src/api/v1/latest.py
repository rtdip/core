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
from fastapi import HTTPException, Depends, Body

from src.sdk.python.rtdip_sdk.queries.time_series import latest
from src.api.v1.models import (
    BaseQueryParams,
    BaseHeaders,
    MetadataQueryParams,
    TagsBodyParams,
    LatestResponse,
    LimitOffsetQueryParams,
    HTTPError,
)
from src.api.auth.azuread import oauth2_scheme
from src.api.v1.common import common_api_setup_tasks, json_response, lookup_before_get
from src.api.FastAPIApp import api_v1_router


def latest_retrieval_get(
    query_parameters, metadata_query_parameters, limit_offset_parameters, base_headers
):
    try:
        (connection, parameters) = common_api_setup_tasks(
            query_parameters,
            metadata_query_parameters=metadata_query_parameters,
            limit_offset_query_parameters=limit_offset_parameters,
            base_headers=base_headers,
        )

        if all(
            (key in parameters and parameters[key] != None)
            for key in ["business_unit", "asset", "data_security_level"]
        ):
            # if have all required params, run normally
            data = latest.get(connection, parameters)
        else:
            # else wrap in lookup function that finds tablenames and runs function (if mutliple tables, handles concurrent requests)
            data = lookup_before_get("latest", connection, parameters)

        return json_response(data, limit_offset_parameters)
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))


get_description = """
## Latest 

Retrieval of latest event values for a given tag name or list of tag names.
"""


@api_v1_router.get(
    path="/events/latest",
    name="Latest GET",
    description=get_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": LatestResponse}, 400: {"model": HTTPError}},
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Latest Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/latest/",
        }
    },
)
async def latest_get(
    query_parameters: BaseQueryParams = Depends(),
    metadata_query_parameters: MetadataQueryParams = Depends(),
    limit_offset_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return latest_retrieval_get(
        query_parameters,
        metadata_query_parameters,
        limit_offset_parameters,
        base_headers,
    )


post_description = """
## Metadata 

Retrieval of latest event values for a given tag name or list of tag names via a POST method to enable providing a list of tag names that can exceed url length restrictions via GET Query Parameters.
"""


@api_v1_router.post(
    path="/events/latest",
    name="Latest POST",
    description=post_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": LatestResponse}, 400: {"model": HTTPError}},
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Latest Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/latest/",
        }
    },
)
async def latest_post(
    query_parameters: BaseQueryParams = Depends(),
    metadata_query_parameters: TagsBodyParams = Body(default=...),
    limit_offset_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return latest_retrieval_get(
        query_parameters,
        metadata_query_parameters,
        limit_offset_parameters,
        base_headers,
    )
