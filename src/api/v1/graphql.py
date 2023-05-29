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

import strawberry
import os
from strawberry.fastapi import GraphQLRouter
from src.api.v1.models import RawResponseQL
import logging
from typing import Any
from pandas.io.json import build_table_schema
from fastapi import Query, HTTPException, Header, Depends
from typing import List
from datetime import date
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.queries import raw
from src.api.v1.models import RawResponse
from src.api.auth import azuread
import nest_asyncio
nest_asyncio.apply()

@strawberry.type
class Query:
  @strawberry.field
  async def raw_get(
      business_unit: str = Query(..., description="Business Unit Name"), 
      region: str = Query(..., description="Region"), 
      asset: str = Query(..., description="Asset"), 
      data_security_level: str = Query(..., description="Data Security Level"), 
      data_type: str = Query(..., description="Data Type", examples={"float": {"value": "float"}, "integer": {"value": "integer"}, "string": {"value": "string"}}), 
      tag_name: List[str] = Query(..., description="Tag Name"),
      include_bad_data: bool = Query(True, description="Include or remove Bad data points"),             
      start_date: date = Query(..., description="Start Date", example="2022-01-01"),
      end_date: date = Query(..., description="End Date", example="2022-01-02"),
      authorization: str = Header(None, include_in_schema=False),
      # authorization: str = Depends(oauth2_scheme)
      ) -> RawResponseQL:
    try:
        token = azuread.get_azure_ad_token(authorization)
        
        connection = DatabricksSQLConnection(os.environ.get("DATABRICKS_SQL_SERVER_HOSTNAME"), os.environ.get("DATABRICKS_SQL_HTTP_PATH"), token)

        parameters = {
            "business_unit": business_unit,
            "region": region,
            "asset": asset,
            "data_security_level": data_security_level,
            "data_type": data_type,
            "tag_names": tag_name,
            "include_bad_data": include_bad_data,
            "start_date": str(start_date),
            "end_date": str(end_date),
        }
        data = raw.get(connection, parameters)
        return RawResponse(schema=build_table_schema(data, index=False, primary_key=False), data=data.to_dict(orient="records"))
    except Exception as e:
        logging.error(str(e))
        return HTTPException(status_code=500, detail=str(e))

schema = strawberry.Schema(Query)
        
graphql_router = GraphQLRouter(schema)