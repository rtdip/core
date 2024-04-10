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

from fastapi import APIRouter, FastAPI
from fastapi.responses import RedirectResponse
from fastapi.openapi.docs import (
    get_swagger_ui_html,
    get_redoc_html,
    get_swagger_ui_oauth2_redirect_html,
)
from fastapi.middleware.gzip import GZipMiddleware
import os

api_v1_router = APIRouter(prefix="/api/v1")
TITLE = "Real Time Data Ingestion Platform"

tags_metadata = [
    {
        "name": "Events",
        "description": "Retrieval of timeseries data with options to resample and interpolate the result.",
    },
    {
        "name": "Metadata",
        "description": "Contextual metadata about timeseries events",
    },
    {
        "name": "SQL",
        "description": "Retrieval of data using SQL queries",
    },
]

description = """
<p align="center"><img src=https://raw.githubusercontent.com/rtdip/core/develop/docs/getting-started/images/rtdip-horizontal-color.png alt='rtdip' width=25% height=25%/></p>

<p align="center"> <b> APIs to interact with Real Time Data Ingestion Platform </b></p>

## Authentication

The below APIs use OAuth 2.0 authentication and Bearer tokens for authentication. For more information, please refer to our [documentation](https://www.rtdip.io/api/authentication/) for further information and examples on how to authenticate with the APIs.

### Azure Active Directory OAuth 2.0

Important Azure AD Values for Authentication are below.

| Parameter | Value |
|-----------|-------|
| Token Url | https://login.microsoftonline.com/{}/oauth2/v2.0/token |
| Scope     | 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default |

## Documentation

Please refer to the following links for further information about these APIs and RTDIP in general:

[ReDoc](/redoc)

[Real Time Data Ingestion Platform](https://www.rtdip.io/)
""".format(
    os.environ.get("TENANT_ID")
)

app = FastAPI(
    title=TITLE,
    description=description,
    version="1.0.0",
    openapi_tags=tags_metadata,
    openapi_url="/api/openapi.json",
    docs_url=None,
    redoc_url=None,
    license_info={"name": "Apache License 2.0", "identifier": "Apache-2.0"},
)

app.add_middleware(GZipMiddleware, minimum_size=1000)


@app.get("/", include_in_schema=False)
async def home():
    return RedirectResponse(url="/docs")


@app.get("/docs", include_in_schema=False)
async def swagger_ui_html():
    client_id = os.getenv("MICROSOFT_PROVIDER_AUTHENTICATION_ID")
    return get_swagger_ui_html(
        openapi_url="api/openapi.json",
        title=TITLE + " - Swagger",
        swagger_favicon_url="https://raw.githubusercontent.com/rtdip/core/develop/docs/assets/favicon.png",
        init_oauth={
            "usePkceWithAuthorizationCodeGrant": True,
            "clientId": client_id,
            "scopes": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default",
        },
        oauth2_redirect_url=app.swagger_ui_oauth2_redirect_url,
    )


@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()


@app.get("/redoc", include_in_schema=False)
async def redoc_ui_html():
    return get_redoc_html(
        openapi_url="api/openapi.json",
        title=TITLE + " - ReDoc",
        redoc_favicon_url="https://raw.githubusercontent.com/rtdip/core/develop/docs/assets/favicon.png",
    )
