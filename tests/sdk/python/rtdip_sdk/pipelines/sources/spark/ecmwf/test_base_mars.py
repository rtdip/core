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
import numpy as np

from pytest_mock import MockerFixture
from unittest.mock import Mock, patch
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.ecmwf.base_mars import (
    SparkECMWFBaseMarsSource,
)

# Sample test data
date_start = "2020-10-01 00:00:00"
date_end = "2020-10-02 00:00:00"
save_path = "/path/to/save"
ecmwf_api_key = "1234567890"
ecmwf_api_email = "john.smith@email.com"


def test_retrieve():
    api_instance = SparkECMWFBaseMarsSource(
        date_start, date_end, save_path, ecmwf_api_key, ecmwf_api_email
    )

    ec_vars = ["10u", "10v"]
    np.array(ec_vars)

    lead_times = [*range(91), *range(93, 146, 3), *range(150, 246, 6)]
    np.array(lead_times)

    mars_req = {
        "class": "od",  # ecmwf classification of data
        "stream": "oper",  # operational model
        "expver": "1",  # experiment version of data
        "levtype": "sfc",  # surface level forecasts
        "type": "fc",  # forecasts
        "param": ec_vars,  # variables
        "step": lead_times,  # which lead times?
        "area": [73.5, -27, 33, 45],  # N/W/S/E
        "grid": [0.1, 0.1],  # grid res of output
    }

    result = api_instance.retrieve(mars_dict=mars_req, tries=5, n_jobs=-1)

    assert isinstance(result, object)


def test_info_after_retrieve():
    api_instance = SparkECMWFBaseMarsSource(
        date_start, date_end, save_path, ecmwf_api_key, ecmwf_api_email
    )

    ec_vars = ["10u", "10v"]
    np.array(ec_vars)

    lead_times = [*range(91), *range(93, 146, 3), *range(150, 246, 6)]
    np.array(lead_times)

    mars_req = {
        "class": "od",  # ecmwf classification of data
        "stream": "oper",  # operational model
        "expver": "1",  # experiment version of data
        "levtype": "sfc",  # surface level forecasts
        "type": "fc",  # forecasts
        "param": ec_vars,  # variables
        "step": lead_times,  # which lead times?
        "area": [73.5, -27, 33, 45],  # N/W/S/E
        "grid": [0.1, 0.1],  # grid res of output
    }

    api_instance.retrieve(mars_dict=mars_req, tries=5, n_jobs=-1)

    info = api_instance.info()

    assert isinstance(info, pd.Series)
    assert len(info) == len(api_instance.dates)
    assert all(info == api_instance.success)
