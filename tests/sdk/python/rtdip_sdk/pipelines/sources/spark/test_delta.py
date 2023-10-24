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
from importlib_metadata import version
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
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta import SparkDeltaSource
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pytest_mock import MockerFixture


def test_spark_delta_read_setup(spark_session: SparkSession):
    delta_source = SparkDeltaSource(spark_session, {}, "test_spark_delta_read_setup")
    assert delta_source.system_type().value == 2
    delta_spark_artifact_id = "delta-core_2.12"
    if (
        Version.compare(
            _get_python_package_version("delta-spark"), Version.parse("3.0.0")
        )
        >= 0
    ):
        delta_spark_artifact_id = "delta-spark_2.12"
    assert delta_source.libraries() == Libraries(
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
    assert isinstance(delta_source.settings(), dict)
    assert delta_source.pre_read_validation()
    assert delta_source.post_read_validation()


def test_spark_delta_read_batch(spark_session: SparkSession):
    df = spark_session.createDataFrame([{"id": "1"}])
    delta_destination = SparkDeltaDestination(
        df, {}, "test_spark_delta_read_batch", "overwrite"
    )
    delta_source = SparkDeltaSource(spark_session, {}, "test_spark_delta_read_batch")
    delta_destination.write_batch()
    actual_df = delta_source.read_batch()
    assert isinstance(actual_df, DataFrame)
    assert actual_df.schema == StructType([StructField("id", StringType(), True)])


def test_spark_delta_read_stream(spark_session: SparkSession, mocker: MockerFixture):
    delta_source = SparkDeltaSource(spark_session, {}, "test_spark_delta_read_stream")
    expected_df = spark_session.createDataFrame([{"a": "x"}])
    mocker.patch.object(
        delta_source,
        "spark",
        new_callable=mocker.PropertyMock(
            return_value=mocker.Mock(
                readStream=mocker.Mock(
                    format=mocker.Mock(
                        return_value=mocker.Mock(
                            options=mocker.Mock(
                                return_value=mocker.Mock(
                                    load=mocker.Mock(return_value=expected_df)
                                )
                            )
                        )
                    )
                )
            )
        ),
    )
    assert delta_source.pre_read_validation()
    df = delta_source.read_stream()
    assert isinstance(df, DataFrame)
    assert delta_source.post_read_validation()


def test_spark_delta_read_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    delta_source = SparkDeltaSource(spark_session, {}, "test_spark_delta_read_batch")
    mocker.patch.object(
        delta_source,
        "spark",
        new_callable=mocker.PropertyMock(
            return_value=mocker.Mock(
                read=mocker.Mock(
                    format=mocker.Mock(
                        return_value=mocker.Mock(
                            options=mocker.Mock(
                                return_value=mocker.Mock(
                                    table=mocker.Mock(side_effect=Exception)
                                )
                            )
                        )
                    )
                )
            )
        ),
    )

    with pytest.raises(Exception):
        delta_source.read_batch()


def test_spark_delta_read_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    delta_source = SparkDeltaSource(spark_session, {}, "test_spark_delta_read_stream")
    mocker.patch.object(
        delta_source,
        "spark",
        new_callable=mocker.PropertyMock(
            return_value=mocker.Mock(
                readStream=mocker.Mock(
                    format=mocker.Mock(
                        return_value=mocker.Mock(
                            options=mocker.Mock(
                                return_value=mocker.Mock(
                                    load=mocker.Mock(side_effect=Exception)
                                )
                            )
                        )
                    )
                )
            )
        ),
    )

    with pytest.raises(Exception):
        delta_source.read_stream()
