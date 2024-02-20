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
from src.sdk.python.rtdip_sdk.queries.sql.sql_query import SQLQueryBuilder
from src.api.v1.models import (
    BaseHeaders,
    BaseQueryParams,
    SqlBodyParams,
    SqlResponse,
    LimitOffsetQueryParams,
    HTTPError,
    PaginationRow,
)
from src.api.auth.azuread import oauth2_scheme
from src.api.FastAPIApp import api_v1_router
import src.api.v1.common

nest_asyncio.apply()


def sql_get(
    base_query_parameters,
    sql_query_parameters,
    limit_offset_parameters,
    base_headers,
):
    try:
        (connection, parameters) = src.api.v1.common.common_api_setup_tasks(
            base_query_parameters,
            sql_query_parameters=sql_query_parameters,
            limit_offset_query_parameters=limit_offset_parameters,
            base_headers=base_headers,
        )

        limit = None if "limit" in parameters else int(parameters["limit"])
        offset = None if "offset" in parameters else int(parameters["offset"])
        data = SQLQueryBuilder().get(
            connection, parameters["sql_statement"], limit, offset
        )

        pagination = None

        if (
            limit_offset_parameters.limit is not None
            and limit_offset_parameters.offset is not None
        ):
            next = None

            if len(data.index) == limit_offset_parameters.limit:
                next = limit_offset_parameters.offset + limit_offset_parameters.limit

            pagination = PaginationRow(
                limit=limit_offset_parameters.limit,
                offset=limit_offset_parameters.offset,
                next=next,
            )

        return SqlResponse(
            schema=build_table_schema(data, index=False, primary_key=False),
            data=data.replace({np.nan: None}).to_dict(orient="records"),
            pagination=pagination,
        )
    except Exception as e:
        logging.error(str(e))
        raise HTTPException(status_code=400, detail=str(e))


post_description = """
## Sql 

Retrieval of data via a POST method to enable execution of generic SQL statements.
"""


@api_v1_router.post(
    path="/sql/execute",
    name="Sql Execute POST",
    description=post_description,
    tags=["SQL"],
    dependencies=[Depends(oauth2_scheme)],
    responses={200: {"model": SqlResponse}, 400: {"model": HTTPError}},
    openapi_extra={
        "externalDocs": {
            "description": "RTDIP SQL Query Documentation",
            "url": "https://www.rtdip.io/sdk/code-reference/query/functions/sql/sql_query_builder/",
        }
    },
)
async def raw_post(
    base_query_parameters: BaseQueryParams = Depends(),
    sql_query_parameters: SqlBodyParams = Body(default=...),
    limit_offset_query_parameters: LimitOffsetQueryParams = Depends(),
    base_headers: BaseHeaders = Depends(),
):
    return sql_get(
        base_query_parameters,
        sql_query_parameters,
        limit_offset_query_parameters,
        base_headers,
    )
