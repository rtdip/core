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
import os
from fastapi import HTTPException, Depends, Body  # , JSONResponse

from src.api.v1.models import (
    BaseQueryParams,
    BaseHeaders,
    BatchBodyParams,
    BatchResponse,
    LimitOffsetQueryParams,
    HTTPError,
)
from src.api.auth.azuread import oauth2_scheme
from src.api.v1.common import (
    common_api_setup_tasks,
    json_response_batch,
    lookup_before_get,
)
from src.api.FastAPIApp import api_v1_router
from src.api.v1.common import lookup_before_get
from concurrent.futures import *


ROUTE_FUNCTION_MAPPING = {
    "/api/v1/events/raw": "raw",
    "/api/v1/events/latest": "latest",
    "/api/v1/events/resample": "resample",
    "/api/v1/events/plot": "plot",
    "/api/v1/events/interpolate": "interpolate",
    "/api/v1/events/interpolationattime": "interpolationattime",
    "/api/v1/events/circularaverage": "circularaverage",
    "/api/v1/events/circularstandarddeviation": "circularstandarddeviation",
    "/api/v1/events/timeweightedaverage": "timeweightedaverage",
    "/api/v1/events/summary": "summary",
    "/api/v1/events/metadata": "metadata",
    "/api/v1/sql/execute": "execute",
}


async def batch_events_get(
    base_query_parameters, base_headers, batch_query_parameters, limit_offset_parameters
):

    try:

        (connection, parameters) = common_api_setup_tasks(
            base_query_parameters=base_query_parameters,
            base_headers=base_headers,
        )

        # Validate the parameters
        parsed_requests = []
        for request in batch_query_parameters.requests:

            # If required, combine request body and parameters:
            parameters = request["params"]
            if request["method"] == "POST":
                if request["body"] == None:
                    raise Exception(
                        "Incorrectly formatted request provided: All POST requests require a body"
                    )
                parameters = {**parameters, **request["body"]}

            # Map the url to a specific function
            try:
                func = ROUTE_FUNCTION_MAPPING[request["url"]]
            except:
                raise Exception(
                    "Unsupported url: Only relative base urls are supported. Please provide any parameters in the params key"
                )

            # Rename tag_name to tag_names, if required
            if "tag_name" in parameters.keys():
                parameters["tag_names"] = parameters.pop("tag_name")

            # Append to array
            parsed_requests.append({"func": func, "parameters": parameters})

        # Obtain max workers from environment var, otherwise default to one less than cpu count
        max_workers = os.environ.get("BATCH_THREADPOOL_WORKERS", os.cpu_count() - 1)

        # Request the data for each concurrently with threadpool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Use executor.map to preserve order
            results = executor.map(
                lambda arguments: lookup_before_get(*arguments),
                [
                    (parsed_request["func"], connection, parsed_request["parameters"])
                    for parsed_request in parsed_requests
                ],
            )

        return json_response_batch(results)

    except Exception as e:
        print(e)
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))


post_description = """
## Batch 

Retrieval of timeseries data via a POST method to enable providing a list of requests including the route and parameters
"""


@api_v1_router.post(
    path="/events/batch",
    name="Batch POST",
    description=post_description,
    tags=["Events"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": BatchResponse}, 400: {"model": HTTPError}},
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP Batch Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/time_series/batch/",
        }
    },
)
async def batch_post(
    base_query_parameters: BaseQueryParams = Depends(),
    batch_query_parameters: BatchBodyParams = Body(default=...),
    base_headers: BaseHeaders = Depends(),
    limit_offset_query_parameters: LimitOffsetQueryParams = Depends(),
):
    return await batch_events_get(
        base_query_parameters,
        base_headers,
        batch_query_parameters,
        limit_offset_query_parameters,
    )
