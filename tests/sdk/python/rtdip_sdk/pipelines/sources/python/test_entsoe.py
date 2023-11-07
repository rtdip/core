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

import sys

sys.path.insert(0, ".")
from src.sdk.python.rtdip_sdk.pipelines.sources.python.entsoe import PythonEntsoeSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pytest_mock import MockerFixture
from entsoe import EntsoePandasClient
import pandas as pd
import pytest

tz = "UTC"
api_key = "api-key"
start = "20230101"
end = "20231001"
country_code = "NL"


def test_python_entsoe_setup():
    entsoe_source = PythonEntsoeSource(api_key, start, end, country_code)
    assert entsoe_source.system_type().value == 1
    assert entsoe_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(entsoe_source.settings(), dict)
    assert entsoe_source.pre_read_validation()
    assert entsoe_source.post_read_validation()


def test_python_entsoe_read_batch(mocker: MockerFixture):
    entsoe_source = PythonEntsoeSource(api_key, start, end, country_code)
    mocker.patch.object(
        EntsoePandasClient,
        "query_day_ahead_prices",
        return_value=pd.Series(
            {pd.to_datetime("2023-01-01 00:00:00").tz_localize(tz): 123.4}
        ),
    )
    df = entsoe_source.read_batch()
    assert isinstance(df, pd.DataFrame)


def test_entsoe_read_batch_fails(mocker: MockerFixture):
    entsoe_source = PythonEntsoeSource(api_key, start, end, country_code)
    mocker.patch.object(
        EntsoePandasClient, "query_day_ahead_prices", side_effect=Exception
    )

    with pytest.raises(Exception):
        entsoe_source.read_batch()


def test_python_entsoe_read_stream():
    entsoe_source = PythonEntsoeSource(api_key, start, end, country_code)
    with pytest.raises(NotImplementedError) as e:
        entsoe_source.read_stream()
    assert str(e.value) == "ENTSO-E connector does not support the stream operation."
