import logging
from src.api.FastAPIApp import api_v1_router
from fastapi import HTTPException, Depends, Body
import nest_asyncio
import json
from src.sdk.python.rtdip_sdk.functions import time_weighted_average
from src.api.v1.models import BaseQueryParams, ResampleInterpolateResponse, HTTPError, RawQueryParams, TagsQueryParams, TagsBodyParams, TimeWeightedAverageQueryParams
import src.api.v1.common

nest_asyncio.apply()

def time_weighted_average_events_get(base_query_parameters, raw_query_parameters, tag_query_parameters, time_weighted_average_parameters):
    try:
        (connection, parameters) = src.api.v1.common.common_api_setup_tasks(
            base_query_parameters, 
            raw_query_parameters=raw_query_parameters,
            tag_query_parameters=tag_query_parameters,
            time_weighted_average_query_parameters=time_weighted_average_parameters
        )

        data = time_weighted_average.get(connection, parameters)
        data = data.reset_index()
        response = data.to_json(orient="table", index=False, date_unit="us")
        return ResampleInterpolateResponse(**json.loads(response))
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))

get_description = """
## Time Weighted Average 

Time weighted average of raw timeseries data. Refer to the following [documentation](https://www.rtdip.io/sdk/code-reference/time-weighted-average/) for further information.
"""

@api_v1_router.get(
    path="/events/timeweightedaverage",
    name="Time Weighted Average GET",
    description=get_description, 
    tags=["Events"], 
    responses={200: {"model": ResampleInterpolateResponse}, 400: {"model": HTTPError}}
)
async def time_weighted_average_get(
        base_query_parameters: BaseQueryParams = Depends(), 
        raw_query_parameters: RawQueryParams = Depends(),
        tag_query_parameters: TagsQueryParams = Depends(),
        time_weighted_average_parameters: TimeWeightedAverageQueryParams = Depends()
    ):
    return time_weighted_average_events_get(base_query_parameters, raw_query_parameters, tag_query_parameters, time_weighted_average_parameters)

post_description = """
## Time Weighted Average 

Time weighted average of raw timeseries data via a POST method to enable providing a list of tag names that can exceed url length restrictions via GET Query Parameters. Refer to the following [documentation](https://ssip-docs.shell.com/sdk/code-reference/interpolate/) for further information.
"""

@api_v1_router.post(
    path="/events/timeweightedaverage",
    name="Time Weighted Average POST",
    description=get_description, 
    tags=["Events"], 
    responses={200: {"model": ResampleInterpolateResponse}, 400: {"model": HTTPError}}
)
async def time_weighted_average_post(
        base_query_parameters: BaseQueryParams = Depends(), 
        raw_query_parameters: RawQueryParams = Depends(),
        tag_query_parameters: TagsBodyParams = Body(default=...),
        time_weighted_average_parameters: TimeWeightedAverageQueryParams = Depends()
    ):
    return time_weighted_average_events_get(base_query_parameters, raw_query_parameters, tag_query_parameters, time_weighted_average_parameters)    