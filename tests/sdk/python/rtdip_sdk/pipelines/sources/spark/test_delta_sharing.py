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
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import (
    SparkDeltaDestination,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta_sharing import (
    SparkDeltaSharingSource,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pytest_mock import MockerFixture


def test_spark_delta_sharing_read_setup(spark_session: SparkSession):
    delta_sharing_source = SparkDeltaSharingSource(
        spark_session, {}, "test_databricks_delta_read_setup"
    )
    assert delta_sharing_source.system_type().value == 2
    assert delta_sharing_source.libraries() == Libraries(
        maven_libraries=[
            MavenLibrary(
                group_id="io.delta",
                artifact_id="delta-sharing-spark_2.12",
                version="1.0.0",
            )
        ],
        pypi_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(delta_sharing_source.settings(), dict)
    assert delta_sharing_source.pre_read_validation()
    assert delta_sharing_source.post_read_validation()


def test_spark_delta_sharing_read_batch(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    delta_destination = SparkDeltaDestination(
        expected_df, {}, "test_spark_delta_sharing_read_batch", "overwrite"
    )
    delta_sharing_source = SparkDeltaSharingSource(
        spark_session, {}, "test_spark_delta_sharing_read_batch"
    )
    delta_destination.write_batch()
    actual_df = delta_sharing_source.read_batch()
    assert isinstance(actual_df, DataFrame)
    assert actual_df.schema == StructType([StructField("id", StringType(), True)])


def test_spark_delta_sharing_read_stream(
    spark_session: SparkSession, mocker: MockerFixture
):
    delta_source = SparkDeltaSharingSource(
        spark_session, {}, "test_spark_delta_read_stream"
    )
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


def test_spark_delta_sharing_read_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    delta_source = SparkDeltaSharingSource(
        spark_session, {}, "test_spark_delta_read_batch"
    )
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


def test_spark_delta_sharing_read_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    delta_source = SparkDeltaSharingSource(
        spark_session, {}, "test_spark_delta_read_stream"
    )
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
