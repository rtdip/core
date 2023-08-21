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
import pytest
from src.sdk.python.rtdip_sdk.pipelines.destinations.python.delta import (
    PythonDeltaDestination,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pytest_mock import MockerFixture
import polars as pl
import pandas as pd

OPTIONS = {"aws_access_key_id": "id", "aws_secret_access_key": "key"}
polars_write_mocked = "polars.DataFrame.write_delta"


def test_python_delta_write_setup():
    data = pl.LazyFrame({"col1": [1, 2], "col2": [3, 4]})
    delta_destination = PythonDeltaDestination(data, "path", {}, "overwrite")
    assert delta_destination.system_type().value == 1
    assert delta_destination.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(delta_destination.settings(), dict)
    assert delta_destination.pre_write_validation()
    assert delta_destination.post_write_validation()


def test_python_delta_write_batch(mocker: MockerFixture):
    mocked_write = mocker.patch(polars_write_mocked, return_value=None)

    data = pl.LazyFrame({"col1": [1, 2], "col2": [3, 4]})

    delta_destination = PythonDeltaDestination(data=data, path="path", mode="overwrite")
    actual = delta_destination.write_batch()

    mocked_write.assert_called_once
    assert actual is None


def test_python_delta_write_batch_with_options(mocker: MockerFixture):
    mocked_write = mocker.patch(polars_write_mocked, return_value=None)

    data = pl.LazyFrame({"col1": [1, 2], "col2": [3, 4]})

    delta_destination = PythonDeltaDestination(
        data=data, path="path", options=OPTIONS, mode="overwrite"
    )
    actual = delta_destination.write_batch()

    mocked_write.assert_called_once
    assert actual is None


def test_python_delta_write_batch_fails(mocker: MockerFixture):
    mocker.patch(polars_write_mocked, side_effect=Exception)

    data = pl.LazyFrame({"col1": [1, 2], "col2": [3, 4]})
    delta_destination = PythonDeltaDestination(data=data, path="path", mode="overwrite")

    with pytest.raises(Exception):
        delta_destination.write_batch()


def test_python_delta_write_batch_with_options_fails(mocker: MockerFixture):
    mocker.patch(polars_write_mocked, side_effect=Exception)

    data = pl.LazyFrame({"col1": [1, 2], "col2": [3, 4]})
    delta_destination = PythonDeltaDestination(
        data=data, path="path", options=OPTIONS, mode="overwrite"
    )

    with pytest.raises(Exception):
        delta_destination.write_batch()


def test_python_delta_write_batch_type_fails():
    with pytest.raises(ValueError) as excinfo:
        data = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
        delta_destination = PythonDeltaDestination(
            data=data, path="path", options=OPTIONS, mode="overwrite"
        )
        delta_destination.write_batch()

    assert (
        str(excinfo.value)
        == "Data must be a Polars LazyFrame. See https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html"
    )


def test_python_delta_write_stream():
    with pytest.raises(NotImplementedError) as excinfo:
        data = pl.LazyFrame({"col1": [1, 2], "col2": [3, 4]})
        delta_destination = PythonDeltaDestination(
            data=data, path="path", options=OPTIONS, mode="overwrite"
        )
        delta_destination.write_stream()

    assert (
        str(excinfo.value)
        == "Writing to a Delta table using Python is only possible for batch writes. To perform a streaming read, use the write_stream method of the SparkDeltaDestination component"
    )
