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
from src.sdk.python.rtdip_sdk.pipelines.sources.python.mffbas import PythonMFFBASSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pytest_mock import MockerFixture
from requests import HTTPError
import pandas as pd
import pandas.api.types as ptypes
import pytest

start = "2023-10-30"
end = "2023-10-31"


def test_python_mffbas_setup():
    sjv_source = PythonMFFBASSource(start, end)
    assert sjv_source.system_type().value == 1
    assert sjv_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(sjv_source.settings(), dict)
    assert sjv_source.pre_read_validation()
    assert sjv_source.post_read_validation()


def test_python_mffbas_read_batch(mocker: MockerFixture):
    sjv_source = PythonMFFBASSource(start, end)
    sample_json = {
        "DateAndOrTime": {
            "startDateTime": "2023-10-29T23:00:00Z",
            "endDateTime": "2023-10-30T23:00:00Z",
        },
        "Detail_SeriesList": [
            {
                "direction": "E17",
                "calendar_date": "2023-10-30",
                "PointList": [
                    {"pos": 1, "qnt": 3.1e-05},
                    {"pos": 2, "qnt": 3.2e-05},
                    {"pos": 3, "qnt": 3.3e-05},
                    {"pos": 4, "qnt": 3.4e-05},
                    {"pos": 5, "qnt": 3.5e-05},
                ],
                "pFdate_version": "2023-05-21T22:00:00Z",
                "profileCategory": "E1A",
                "determinedConsumption": "AMI",
                "resolution": "PT15M",
                "profileStatus_quality": "STD",
            }
        ],
    }

    class MockResponse:
        status_code = 200

        def json(self):
            return sample_json

    mocker.patch("requests.request", return_value=MockResponse())

    df = sjv_source.read_batch()

    assert isinstance(df, pd.DataFrame)
    assert all(ptypes.is_float_dtype(df[col]) for col in list(df.columns)[:-1])
    assert ptypes.is_string_dtype(df["year_created"])


def test_mffbas_read_batch_fails(mocker: MockerFixture):
    sjv_source = PythonMFFBASSource(start, end)
    sample_bytes = bytes("Unknown Error".encode("utf-8"))

    class MockResponse:
        status_code = 400
        content = sample_bytes

    mocker.patch("requests.request", return_value=MockResponse())

    with pytest.raises(HTTPError) as e:
        sjv_source.read_batch()
    assert (
        str(e.value)
        == "Unable to access URL `https://gateway.edsn.nl/energyvalues/profile-fractions-series/v1/profile-fractions`. Received status code 400 with message b'Unknown Error'"
    )


def test_python_mffbas_read_stream():
    sjv_source = PythonMFFBASSource(start, end)
    with pytest.raises(NotImplementedError) as e:
        sjv_source.read_stream()
    assert str(e.value) == "MFFBAS connector does not support the stream operation."
