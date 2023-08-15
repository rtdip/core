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
import pandas as pd

import pytest
from unittest.mock import Mock, patch
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.weather import MARS_ECMWF_API

# Sample test data
date_start = "2020-10-01 00:00:00"
date_end = "2020-10-02 00:00:00"
save_path = "/path/to/save"

@pytest.fixture
def api_instance():
    return MARS_ECMWF_API(date_start, date_end, save_path)

def test_retrieve_raises_value_error(api_instance):
    mars_dict = {"date": "2023-08-01"}
    with pytest.raises(ValueError):
        api_instance.retrieve(mars_dict)

def test_info_raises_value_error(api_instance):
    with pytest.raises(ValueError):
        api_instance.info()

def test_retrieve(api_instance, mocker):
    # Mock ECMWFService to simulate successful request
    class MockECMWFService:
        def execute(self, req_dict, target):
            pass
    mocker.patch('ECMWFService', MockECMWFService)
    
    mars_dict = {"param": 123}
    api_instance.retrieve(mars_dict)
    assert all(api_instance.success)

def test_info_after_retrieve(api_instance, mocker):
    # Mock ECMWFService to simulate successful request
    class MockECMWFService:
        def execute(self, req_dict, target):
            pass
    mocker.patch('ECMWFService', MockECMWFService)
    
    mars_dict = {"param": 123}
    api_instance.retrieve(mars_dict)
    
    info = api_instance.info()
    assert isinstance(info, pd.Series)
    assert len(info) == len(api_instance.dates)
    assert all(info == api_instance.success)