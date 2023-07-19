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
from src.sdk.python.rtdip_sdk.queries import metadata
from src.api.v1.models import BaseQueryParams, MetadataQueryParams, TagsBodyParams, MetadataResponse, HTTPError
from src.api.auth.azuread import oauth2_scheme
from src.api.FastAPIApp import api_v1_router
from src.api.v1 import common

nest_asyncio.apply()

def metadata_retrieval_get(query_parameters, metadata_query_parameters):
    try:
        (connection, parameters) = common.common_api_setup_tasks(query_parameters, metadata_query_parameters=metadata_query_parameters)

        data = metadata.get(connection, parameters)
        return MetadataResponse(schema=build_table_schema(data, index=False, primary_key=False), data=data.replace({np.nan: None}).to_dict(orient="records"))
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))

get_description = """
## Metadata 

Retrieval of metadata, including UoM, Description and any other possible fields, if available. Refer to the following [documentation](https://www.rtdip.io/sdk/code-reference/query/metadata/) for further information.
"""

@api_v1_router.get(
    path="/metadata", 
    name="Metadata GET",
    description=get_description,
    tags=["Metadata"], 
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": MetadataResponse}, 400: {"model": HTTPError}}
)
async def metadata_get(query_parameters: BaseQueryParams = Depends(), metadata_query_parameters: MetadataQueryParams = Depends()):
    return metadata_retrieval_get(query_parameters, metadata_query_parameters)

post_description = """
## Metadata 

Retrieval of metadata, including UoM, Description and any other possible fields, if available via a POST method to enable providing a list of tag names that can exceed url length restrictions via GET Query Parameters. Refer to the following [documentation](https://www.rtdip.io/sdk/code-reference/query/metadata/) for further information.
"""

@api_v1_router.post(
    path="/metadata", 
    name="Metadata POST",
    description=post_description,
    tags=["Metadata"], 
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": MetadataResponse}, 400: {"model": HTTPError}}
)
async def metadata_post(query_parameters: BaseQueryParams = Depends(), metadata_query_parameters: TagsBodyParams = Body(default=...)):
    return metadata_retrieval_get(query_parameters, metadata_query_parameters)