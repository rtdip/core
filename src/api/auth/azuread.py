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

from fastapi.security import OAuth2AuthorizationCodeBearer
from src.sdk.python.rtdip_sdk.authentication.authenticate import DefaultAuth

oauth2_scheme = OAuth2AuthorizationCodeBearer(
    authorizationUrl = "https://login.microsoftonline.com/xxxxx/oauth2/v2.0/authorize", 
    tokenUrl= "https://login.microsoftonline.com/xxxxx/oauth2/v2.0/token", 
    refreshUrl="https://login.microsoftonline.com/xxxxx/oauth2/v2.0/refresh",
)

def get_azure_ad_token(authorization = None):
    if authorization == None or authorization == "No Token":
        access_token = DefaultAuth().authenticate()
        token = access_token.get_token("xxxxx").token
    else:
        token = authorization.replace("Bearer ", "")

    return token