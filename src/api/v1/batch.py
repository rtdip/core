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
from src.sdk.python.rtdip_sdk.queries.time_series import batch

from src.api.v1.models import (
    BatchBaseQueryParams,
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
import pandas as pd


ROUTE_FUNCTION_MAPPING = {
    "/events/raw": "raw",
    "/events/latest": "latest",
    "/events/resample": "resample",
    "/events/plot": "plot",
    "/events/interpolate": "interpolate",
    "/events/interpolationattime": "interpolation_at_time",
    "/events/circularaverage": "circular_average",
    "/events/circularstandarddeviation": "circular_standard_deviation",
    "/events/timeweightedaverage": "time_weighted_average",
    "/events/summary": "summary",
    "/events/metadata": "metadata",
    "/sql/execute": "sql",
}


def parse_batch_requests(requests):
    """
    Parse requests into dict of required format of sdk function
        - Unpack request body if post request
        - Map the url to the sdk function
        - Rename tag_name parameter to tag_names
    """

    parsed_requests = []
    for request in requests:

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
                "Unsupported url: Only relative base urls are supported, for example '/events/raw'. Please provide any parameters under the params key in the same format as the sdk"
            )

        # Rename tag_name to tag_names, if required
        if "tag_name" in parameters.keys():
            parameters["tag_names"] = parameters.pop("tag_name")

        # Append to array
        parsed_requests.append({"func": func, "parameters": parameters})

    return parsed_requests


def run_direct_or_lookup(func_name, connection, parameters):
    """
    Runs directly if all params (or SQL function) provided, otherwise uses lookup table
    """
    try:
        if func_name == "sql" or all(
            (key in parameters and parameters[key] != None)
            for key in ["business_unit", "asset"]
        ):
            # Run batch get for single table query if table name provided, or SQL function
            params_list = [{"type": func_name, "parameters_dict": parameters}]
            batch_results = batch.get(connection, params_list, threadpool_max_workers=1)

            # Extract 0th from generator object since only one result
            result = [result for result in batch_results][0]
            return result
        else:
            return lookup_before_get(func_name, connection, parameters)
    except Exception as e:
        # Return a dataframe with an error message if any of requests fail
        return pd.DataFrame([{"Error": str(e)}])


async def batch_events_get(
    base_query_parameters, base_headers, batch_query_parameters, limit_offset_parameters
):

    try:
        # Set up connection
        (connection, parameters) = common_api_setup_tasks(
            base_query_parameters=base_query_parameters,
            base_headers=base_headers,
        )

        # Parse requests into dicts required by sdk
        parsed_requests = parse_batch_requests(batch_query_parameters.requests)

        # Obtain max workers from environment var, otherwise default to 10
        max_workers = os.environ.get("BATCH_THREADPOOL_WORKERS", 10)

        # ensure max_workers is an integer
        max_workers = int(max_workers)

        # Request the data for each concurrently with threadpool
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Use executor.map to preserve order
            results = executor.map(
                lambda arguments: run_direct_or_lookup(*arguments),
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
    base_query_parameters: BatchBaseQueryParams = Depends(),
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
