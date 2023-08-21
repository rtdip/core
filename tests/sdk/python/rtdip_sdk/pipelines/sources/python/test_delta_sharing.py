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
from src.sdk.python.rtdip_sdk.pipelines.sources.python.delta_sharing import (
    PythonDeltaSharingSource,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pytest_mock import MockerFixture
import pytest
import delta_sharing
import pandas as pd
import polars as pl

profile_path = "/path/to/table"
share_name = "share_name"
schema_name = "schema_name"
table_name = "table_name"


def test_python_delta_sharing_setup():
    delta_sharing_source = PythonDeltaSharingSource(
        profile_path, share_name, schema_name, table_name
    )
    assert delta_sharing_source.system_type().value == 1
    assert delta_sharing_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(delta_sharing_source.settings(), dict)
    assert delta_sharing_source.pre_read_validation()
    assert delta_sharing_source.post_read_validation()


def test_python_delta_sharing_read_batch(mocker: MockerFixture):
    delta_sharing_source = PythonDeltaSharingSource(
        profile_path, share_name, schema_name, table_name
    )
    mocker.patch.object(
        delta_sharing,
        "load_as_pandas",
        return_value=pd.DataFrame({"test": ["test_data"]}),
    )
    lf = delta_sharing_source.read_batch()
    assert isinstance(lf, pl.LazyFrame)


def test_delta_sharing_read_batch_fails():
    delta_sharing_source = PythonDeltaSharingSource(
        profile_path, share_name, schema_name, table_name
    )
    with pytest.raises(FileNotFoundError):
        delta_sharing_source.read_batch()


def test_python_delta_sharing_read_stream():
    delta_sharing_source = PythonDeltaSharingSource(
        profile_path, share_name, schema_name, table_name
    )
    with pytest.raises(NotImplementedError):
        delta_sharing_source.read_stream()
