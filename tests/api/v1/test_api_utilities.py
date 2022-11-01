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

from pytest_mock import MockerFixture
from fastapi.testclient import TestClient
from src.api.v1 import app

def test_api_home(mocker: MockerFixture):   
    client = TestClient(app) 
    
    response = client.get("/")

    assert response.status_code == 200

def test_api_docs(mocker: MockerFixture):   
    client = TestClient(app) 
    
    response = client.get("/docs")

    assert response.status_code == 200

def test_api_redoc(mocker: MockerFixture):   
    client = TestClient(app) 
    
    response = client.get("/redoc")

    assert response.status_code == 200    
    