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
from semver.version import Version
import pytest
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import (
    _get_package_version,
    _get_python_package_version,
)
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import (
    SparkDeltaDestination,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture


class TestStreamingQueryClass:
    isActive: bool = False


def test_spark_delta_write_setup():
    delta_destination = SparkDeltaDestination(
        None, {}, "test_delta_destination_setup", "overwrite"
    )
    assert delta_destination.system_type().value == 2
    delta_spark_artifact_id = "delta-core_2.12"
    if (
        Version.compare(
            _get_python_package_version("delta-spark"), Version.parse("3.0.0")
        )
        >= 0
    ):
        delta_spark_artifact_id = "delta-spark_2.12"
    assert delta_destination.libraries() == Libraries(
        maven_libraries=[
            MavenLibrary(
                group_id="io.delta",
                artifact_id=delta_spark_artifact_id,
                version=_get_package_version("delta-spark"),
            )
        ],
        pypi_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(delta_destination.settings(), dict)
    assert delta_destination.pre_write_validation()
    assert delta_destination.post_write_validation()


def test_spark_delta_write_batch(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    delta_destination = SparkDeltaDestination(
        expected_df, {}, "test_spark_delta_write_batch", "overwrite"
    )
    delta_destination.write_batch()
    actual_df = spark_session.table("test_spark_delta_write_batch")
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_spark_delta_write_batch_path(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.write",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                format=mocker.Mock(
                    return_value=mocker.Mock(
                        mode=mocker.Mock(
                            return_value=mocker.Mock(
                                options=mocker.Mock(
                                    return_value=mocker.Mock(
                                        save=mocker.Mock(return_value=None)
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
    delta_destination = SparkDeltaDestination(
        expected_df, {}, destination="/test_spark_delta_write_batch", mode="overwrite"
    )
    write_df = delta_destination.write_batch()
    assert write_df is None


def test_spark_delta_write_stream(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        format=mocker.Mock(
                            return_value=mocker.Mock(
                                queryName=mocker.Mock(
                                    return_value=mocker.Mock(
                                        outputMode=mocker.Mock(
                                            return_value=mocker.Mock(
                                                options=mocker.Mock(
                                                    return_value=mocker.Mock(
                                                        toTable=mocker.Mock(
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
    eventhub_destination = SparkDeltaDestination(
        expected_df, {}, "test_spark_delta_write_stream", "overwrite"
    )
    actual = eventhub_destination.write_stream()
    assert actual is None


def test_spark_delta_write_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.write",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                format=mocker.Mock(
                    return_value=mocker.Mock(
                        mode=mocker.Mock(
                            return_value=mocker.Mock(
                                options=mocker.Mock(
                                    return_value=mocker.Mock(
                                        saveAsTable=mocker.Mock(side_effect=Exception)
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
    eventhub_destination = SparkDeltaDestination(
        expected_df, {}, "test_spark_delta_write_batch", "overwrite"
    )
    with pytest.raises(Exception):
        eventhub_destination.write_batch()


def test_spark_delta_write_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        format=mocker.Mock(
                            return_value=mocker.Mock(
                                queryName=mocker.Mock(
                                    return_value=mocker.Mock(
                                        outputMode=mocker.Mock(
                                            return_value=mocker.Mock(
                                                options=mocker.Mock(
                                                    return_value=mocker.Mock(
                                                        toTable=mocker.Mock(
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
    eventhub_destination = SparkDeltaDestination(
        expected_df, {}, "test_spark_delta_write_stream", "overwrite"
    )
    with pytest.raises(Exception):
        eventhub_destination.write_stream()
