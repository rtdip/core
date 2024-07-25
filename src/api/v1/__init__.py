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

import azure.functions as func
from fastapi import Depends
from src.api.FastAPIApp import app, api_v1_router
from src.api.v1 import (
    metadata,
    raw,
    sql,
    latest,
    resample,
    plot,
    interpolate,
    interpolation_at_time,
    circular_average,
    circular_standard_deviation,
    time_weighted_average,
    circular_average,
    circular_standard_deviation,
    summary,
    batch,
)
from src.api.auth.azuread import oauth2_scheme

app.include_router(api_v1_router)


async def main(req: func.HttpRequest, context: func.Context) -> func.HttpResponse:
    return await func.AsgiMiddleware(app).handle_async(req, context)
