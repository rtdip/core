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
from unittest.mock import patch
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.rest_api import (
    SparkRestAPIDestination,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    PyPiLibrary,
)
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture


class TestStreamingQueryClass:
    isActive: bool = False  # NOSONAR


class MockResponse:
    status_code: str


test_url = "https://test.com/api"


def test_spark_rest_api_write_setup():
    delta_merge_destination = SparkRestAPIDestination(
        None, {}, test_url, {}, 1, parallelism=1
    )
    assert delta_merge_destination.system_type().value == 2
    assert delta_merge_destination.libraries() == Libraries(
        pypi_libraries=[PyPiLibrary(name="requests", version="2.30.0", repo=None)],
        maven_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(delta_merge_destination.settings(), dict)
    assert delta_merge_destination.pre_write_validation()
    assert delta_merge_destination.post_write_validation()


def test_spark_rest_api_write_stream(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        foreachBatch=mocker.Mock(
                            return_value=mocker.Mock(
                                queryName=mocker.Mock(
                                    return_value=mocker.Mock(
                                        outputMode=mocker.Mock(
                                            return_value=mocker.Mock(
                                                options=mocker.Mock(
                                                    return_value=mocker.Mock(
                                                        start=mocker.Mock(
                                                            return_value=TestStreamingQueryClass()
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ),
    )
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    restapi_destination = SparkRestAPIDestination(
        expected_df, {}, test_url, {}, 1, parallelism=1
    )
    actual = restapi_destination.write_stream()
    assert actual is None


def test_spark_rest_api_write_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.destinations.spark.rest_api.SparkRestAPIDestination._api_micro_batch",
        side_effect=Exception,
    )

    expected_df = spark_session.createDataFrame([{"id": "1"}])
    restapi_destination = SparkRestAPIDestination(
        expected_df, {}, test_url, {}, 1, parallelism=1
    )
    with pytest.raises(Exception):
        restapi_destination.write_batch()


def test_spark_rest_api_write_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        foreachBatch=mocker.Mock(
                            return_value=mocker.Mock(
                                queryName=mocker.Mock(
                                    return_value=mocker.Mock(
                                        outputMode=mocker.Mock(
                                            return_value=mocker.Mock(
                                                options=mocker.Mock(
                                                    return_value=mocker.Mock(
                                                        start=mocker.Mock(
                                                            side_effect=Exception
                                                        )
                                                    )
                                                )
                                            )
                                        )
                                    )
                                )
                            )
                        )
                    )
                )
            )
        ),
    )
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    restapi_destination = SparkRestAPIDestination(
        expected_df, {}, test_url, {}, 1, parallelism=1
    )
    with pytest.raises(Exception):
        restapi_destination.write_stream()
