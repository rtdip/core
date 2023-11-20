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
from pyspark.sql.types import StructType, StructField, IntegerType
import pytest
from requests import HTTPError

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import BaseISOSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

iso_configuration = {}
patch_module_name = "requests.get"


def test_base_iso_read_setup(spark_session: SparkSession):
    base_iso_source = BaseISOSource(spark_session, iso_configuration)

    assert base_iso_source.system_type().value == 2
    assert base_iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(base_iso_source.settings(), dict)

    assert base_iso_source.pre_read_validation()
    assert base_iso_source.post_read_validation()


def test_base_iso_read_stream_exception(spark_session: SparkSession):
    with pytest.raises(NotImplementedError) as exc_info:
        base_iso_source = BaseISOSource(spark_session, iso_configuration)
        base_iso_source.read_stream()

    assert (
        str(exc_info.value)
        == "BaseISOSource connector doesn't support stream operation."
    )


def test_base_iso_read_batch(spark_session: SparkSession, mocker: MockerFixture):
    base_iso_source = BaseISOSource(spark_session, iso_configuration)
    sample_bytes = bytes("id\n1".encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 200

    mock_res = MyResponse()

    mocker.patch(patch_module_name, side_effect=lambda url: mock_res)

    df = base_iso_source.read_batch()
    assert df.count() == 1
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(StructType([StructField("id", IntegerType(), True)]))


def test_base_iso_required_options_fails(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        base_iso_source = BaseISOSource(spark_session, iso_configuration)
        base_iso_source.required_options = ["date"]
        base_iso_source.pre_read_validation()

    assert str(exc_info.value) == "Required option `date` is missing."


def test_base_iso_fetch_url_fails(spark_session: SparkSession, mocker: MockerFixture):
    base_iso_source = BaseISOSource(spark_session, iso_configuration)
    sample_bytes = bytes("Unknown Error".encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 401

    mock_res = MyResponse()
    mocker.patch(patch_module_name, side_effect=lambda url: mock_res)

    with pytest.raises(HTTPError) as exc_info:
        base_iso_source.read_batch()

    expected = "Unable to access URL `https://`. Received status code 401 with message b'Unknown Error'"
    assert str(exc_info.value) == expected


def test_base_iso_source_read_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    base_iso_source = BaseISOSource(spark_session, {})

    mocker.patch.object(
        base_iso_source,
        "spark",
        new_callable=mocker.PropertyMock(
            return_value=mocker.Mock(createDataFrame=mocker.Mock(side_effect=Exception))
        ),
    )

    assert base_iso_source.pre_read_validation()

    with pytest.raises(Exception):
        base_iso_source.read_batch()
