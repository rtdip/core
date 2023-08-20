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
from importlib_metadata import version
import pytest
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import _get_package_version
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.kafka import SparkKafkaSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import KAFKA_SCHEMA
import json
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

kafka_configuration_dict = {
    "kafka.bootstrap.servers": "host1:port1,host2:port2",
    "subscribe": "topic1",
}


def test_spark_kafka_read_setup(spark_session: SparkSession):
    kafka_configuration = kafka_configuration_dict
    kafka_source = SparkKafkaSource(spark_session, kafka_configuration)
    assert kafka_source.system_type().value == 2
    assert kafka_source.libraries() == Libraries(
        maven_libraries=[
            MavenLibrary(
                group_id="org.apache.spark",
                artifact_id="spark-sql-kafka-0-10_2.12",
                version=_get_package_version("pyspark"),
            )
        ],
        pypi_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(kafka_source.settings(), dict)
    assert kafka_source.pre_read_validation()
    df = spark_session.createDataFrame(data=[], schema=KAFKA_SCHEMA)
    assert kafka_source.post_read_validation(df)


def test_spark_kafka_read_batch(spark_session: SparkSession):
    kafka_configuration = kafka_configuration_dict
    kafka_source = SparkKafkaSource(spark_session, kafka_configuration)
    assert kafka_source.pre_read_validation()
    df = kafka_source.read_batch()
    assert isinstance(df, DataFrame)
    assert kafka_source.post_read_validation(df)


def test_spark_kafka_read_stream(spark_session: SparkSession):
    kafka_configuration = kafka_configuration_dict
    kafka_source = SparkKafkaSource(spark_session, kafka_configuration)
    assert kafka_source.pre_read_validation()
    df = kafka_source.read_stream()
    assert isinstance(df, DataFrame)
    assert kafka_source.post_read_validation(df)


def test_spark_kafka_read_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    kafka_source = SparkKafkaSource(spark_session, {})
    mocker.patch.object(
        kafka_source,
        "spark",
        new_callable=mocker.PropertyMock(
            return_value=mocker.Mock(
                read=mocker.Mock(
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
    assert kafka_source.pre_read_validation()
    with pytest.raises(Exception):
        kafka_source.read_batch()


def test_spark_kafka_read_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    kafka_source = SparkKafkaSource(spark_session, {})
    mocker.patch.object(
        kafka_source,
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
    assert kafka_source.pre_read_validation()
    with pytest.raises(Exception):
        kafka_source.read_stream()
