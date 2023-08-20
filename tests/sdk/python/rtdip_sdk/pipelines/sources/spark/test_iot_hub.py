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
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iot_hub import SparkIoThubSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import EVENTHUB_SCHEMA
import json
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

iothub_connection_string = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test;EntityPath=test"
iothub_configuration_dict = {
    "eventhubs.connectionString": iothub_connection_string,
    "eventhubs.consumerGroup": "$Default",
    "eventhubs.startingPosition": json.dumps(
        {"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True}
    ),
}


def test_spark_iothub_read_setup(spark_session: SparkSession):
    iothub_configuration = iothub_configuration_dict
    iothub_source = SparkIoThubSource(spark_session, iothub_configuration)
    assert iothub_source.system_type().value == 2
    assert iothub_source.libraries() == Libraries(
        maven_libraries=[
            MavenLibrary(
                group_id="com.microsoft.azure",
                artifact_id="azure-eventhubs-spark_2.12",
                version="2.3.22",
            )
        ],
        pypi_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(iothub_source.settings(), dict)
    assert iothub_source.pre_read_validation()
    df = spark_session.createDataFrame(data=[], schema=EVENTHUB_SCHEMA)
    assert iothub_source.post_read_validation(df)


def test_spark_iothub_read_batch(spark_session: SparkSession):
    iothub_configuration = iothub_configuration_dict
    iothub_source = SparkIoThubSource(spark_session, iothub_configuration)
    assert iothub_source.pre_read_validation()
    df = iothub_source.read_batch()
    assert isinstance(df, DataFrame)
    assert iothub_source.post_read_validation(df)


def test_spark_iothub_read_stream(spark_session: SparkSession):
    iothub_configuration = iothub_configuration_dict
    iothub_source = SparkIoThubSource(spark_session, iothub_configuration)
    assert iothub_source.pre_read_validation()
    df = iothub_source.read_stream()
    assert isinstance(df, DataFrame)
    assert iothub_source.post_read_validation(df)


def test_spark_iothub_read_batch_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    iothub_source = SparkIoThubSource(spark_session, {})
    mocker.patch.object(
        iothub_source,
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
    assert iothub_source.pre_read_validation()
    with pytest.raises(Exception):
        iothub_source.read_batch()


def test_spark_iothub_read_stream_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    iothub_source = SparkIoThubSource(spark_session, {})
    mocker.patch.object(
        iothub_source,
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
    assert iothub_source.pre_read_validation()
    with pytest.raises(Exception):
        iothub_source.read_stream()
