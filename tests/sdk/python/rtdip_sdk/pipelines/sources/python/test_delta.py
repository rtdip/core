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
from src.sdk.python.rtdip_sdk.pipelines.sources.python.delta import PythonDeltaSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pytest_mock import MockerFixture
import pytest
import polars as pl

table_path = "/path/to/table"


def test_python_delta_setup():
    delta_source = PythonDeltaSource(table_path)
    assert delta_source.system_type().value == 1
    assert delta_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(delta_source.settings(), dict)
    assert delta_source.pre_read_validation()
    assert delta_source.post_read_validation()


def test_python_delta_read_batch(mocker: MockerFixture):
    delta_source = PythonDeltaSource(table_path)
    mocker.patch.object(pl, "scan_delta", return_value=pl.LazyFrame())
    lf = delta_source.read_batch()
    assert isinstance(lf, pl.LazyFrame)


def test_delta_read_batch_fails():
    delta_source = PythonDeltaSource(table_path)
    with pytest.raises(FileNotFoundError):
        delta_source.read_batch()


def test_python_delta_read_stream():
    delta_source = PythonDeltaSource(table_path)
    with pytest.raises(NotImplementedError):
        delta_source.read_stream()
