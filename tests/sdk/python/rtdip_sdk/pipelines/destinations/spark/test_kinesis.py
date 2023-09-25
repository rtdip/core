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
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.kinesis import (
    SparkKinesisDestination,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    MavenLibrary,
)
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture


class TestStreamingQueryClass:
    isActive: bool = False


kinesis_configuration_dict = {
    "awsAccessKey": "testKey",
    "awsSecretKey": "testSecret",
    "streamName": "testStream",
    "region": "testRegion",
}


def test_spark_kinesis_write_setup():
    kinesis_configuration = kinesis_configuration_dict
    kinesis_source = SparkKinesisDestination(None, kinesis_configuration)
    assert kinesis_source.system_type().value == 3
    assert kinesis_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(kinesis_source.settings(), dict)
    assert kinesis_source.pre_write_validation()
    assert kinesis_source.post_write_validation()


def test_spark_kinesis_write_batch(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch(
        "pyspark.sql.DataFrame.write",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                format=mocker.Mock(
                    return_value=mocker.Mock(
                        options=mocker.Mock(
                            return_value=mocker.Mock(
                                save=mocker.Mock(return_value=None)
                            )
                        )
                    )
                )
            )
        ),
    )
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    kinesis_destination = SparkKinesisDestination(expected_df, {})
    actual = kinesis_destination.write_batch()
    assert actual is None


def test_spark_kinesis_write_stream(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch(
        "pyspark.sql.DataFrame.writeStream",
        new_callable=mocker.Mock(
            return_value=mocker.Mock(
                trigger=mocker.Mock(
                    return_value=mocker.Mock(
                        format=mocker.Mock(
                            return_value=mocker.Mock(
                                outputMode=mocker.Mock(
                                    return_value=mocker.Mock(
                                        options=mocker.Mock(
                                            return_value=mocker.Mock(
                                                queryName=mocker.Mock(
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
    kinesis_destination = SparkKinesisDestination(
        expected_df, {}, "update", "10 seconds"
    )
    actual = kinesis_destination.write_stream()
    assert actual is None


def test_spark_kinesis_write_batch_fails(
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
                                outputMode=mocker.Mock(
                                    return_value=mocker.Mock(
                                        options=mocker.Mock(
                                            return_value=mocker.Mock(
                                                queryName=mocker.Mock(
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
    kinesis_destination = SparkKinesisDestination(
        expected_df, {}, "update", "10 seconds"
    )
    with pytest.raises(Exception):
        kinesis_destination.write_batch()


def test_spark_kinesis_write_stream_fails(
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
                                outputMode=mocker.Mock(
                                    return_value=mocker.Mock(
                                        options=mocker.Mock(
                                            return_value=mocker.Mock(
                                                queryName=mocker.Mock(
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
    kinesis_destination = SparkKinesisDestination(
        expected_df, {}, "update", "10 seconds"
    )
    with pytest.raises(Exception):
        kinesis_destination.write_stream()
