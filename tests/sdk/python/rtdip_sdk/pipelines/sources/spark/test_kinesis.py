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
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.kinesis import SparkKinesisSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import KINESIS_SCHEMA
import json
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

kinesis_configuration_dict = {
    "awsAccessKey": "testKey",
    "awsSecretKey": "testSecret",
    "streamName": "testStream",
    "region": "testRegion",
}


def test_spark_kinesis_read_setup(spark_session: SparkSession):
    kinesis_configuration = kinesis_configuration_dict
    kinesis_source = SparkKinesisSource(spark_session, kinesis_configuration)
    assert kinesis_source.system_type().value == 3
    assert kinesis_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(kinesis_source.settings(), dict)
    assert kinesis_source.pre_read_validation()
    df = spark_session.createDataFrame(data=[], schema=KINESIS_SCHEMA)
    assert kinesis_source.post_read_validation(df)


def test_spark_kinesis_read_batch(spark_session: SparkSession):
    with pytest.raises(NotImplementedError) as excinfo:
        kinesis_source = SparkKinesisSource(spark_session, kinesis_configuration_dict)
        kinesis_source.read_batch()
    assert (
        str(excinfo.value)
        == "Kinesis only supports streaming reads. To perform a batch read, use the read_stream method and specify Trigger on the write_stream as `availableNow=True`"
    )


def test_spark_kinesis_read_stream(spark_session: SparkSession, mocker: MockerFixture):
    kinesis_configuration = kinesis_configuration_dict
    kinesis_source = SparkKinesisSource(spark_session, kinesis_configuration)
    expected_df = spark_session.createDataFrame(data=[], schema=KINESIS_SCHEMA)
    mocker.patch.object(
        kinesis_source,
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
    assert kinesis_source.pre_read_validation()
    df = kinesis_source.read_stream()
    assert isinstance(df, DataFrame)
    assert kinesis_source.post_read_validation(df)


def test_spark_kinesis_read_batch_fails(spark_session: SparkSession):
    kinesis_configuration = kinesis_configuration_dict
    kinesis_source = SparkKinesisSource(spark_session, kinesis_configuration)
    with pytest.raises(Exception):
        kinesis_source.read_batch()


def test_spark_kinesis_read_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    kinesis_configuration = kinesis_configuration_dict
    kinesis_source = SparkKinesisSource(spark_session, kinesis_configuration)
    mocker.patch.object(
        kinesis_source,
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
        kinesis_source.read_stream()
