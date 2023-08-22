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

import pytest
from requests import HTTPError

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.the_weather_company.base_weather import (
    SparkWeatherCompanyBaseWeatherSource,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

configuration = {}


def test_base_weather_read_setup(spark_session: SparkSession):
    base_weather_source = SparkWeatherCompanyBaseWeatherSource(
        spark_session, configuration
    )

    assert base_weather_source.system_type().value == 2
    assert base_weather_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(base_weather_source.settings(), dict)

    assert base_weather_source.pre_read_validation()
    assert base_weather_source.post_read_validation()


def test_weather_iso_read_stream_exception(spark_session: SparkSession):
    with pytest.raises(NotImplementedError) as exc_info:
        base_weather_source = SparkWeatherCompanyBaseWeatherSource(
            spark_session, configuration
        )
        base_weather_source.read_stream()

    assert (
        str(exc_info.value)
        == "SparkWeatherCompanyBaseWeatherSource connector doesn't support stream operation."
    )


def test_weather_iso_required_options_fails(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        base_weather_source = SparkWeatherCompanyBaseWeatherSource(
            spark_session, configuration
        )
        base_weather_source.required_options = ["lat"]
        base_weather_source.pre_read_validation()

    assert str(exc_info.value) == "Required option `lat` is missing."


def test_weather_iso_fetch_url_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    base_weather_source = SparkWeatherCompanyBaseWeatherSource(
        spark_session, configuration
    )
    sample_bytes = bytes("Unknown Error".encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 401

    mock_res = MyResponse()
    mocker.patch("requests.get", side_effect=lambda url, params: mock_res)

    with pytest.raises(HTTPError) as exc_info:
        base_weather_source.read_batch()

    expected = "Unable to access URL `https://`. Received status code 401 with message b'Unknown Error'"
    assert str(exc_info.value) == expected


def test_weather_iso_read_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    base_weather_source = SparkWeatherCompanyBaseWeatherSource(spark_session, {})

    mocker.patch.object(
        base_weather_source,
        "spark",
        new_callable=mocker.PropertyMock(
            return_value=mocker.Mock(createDataFrame=mocker.Mock(side_effect=Exception))
        ),
    )

    assert base_weather_source.pre_read_validation()

    with pytest.raises(Exception):
        base_weather_source.read_batch()
