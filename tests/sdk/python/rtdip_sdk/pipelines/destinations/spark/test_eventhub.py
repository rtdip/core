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
sys.path.insert(0, '.')
import pytest 
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.eventhub import SparkEventhubDestination
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries, MavenLibrary
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

class TestStreamingQueryClass():
    isActive: bool = False  # NOSONAR

def test_spark_eventhub_write_setup():
    eventhub_destination = SparkEventhubDestination(None, {})
    assert eventhub_destination.system_type().value == 2
    assert eventhub_destination.libraries() == Libraries(maven_libraries=[MavenLibrary(
                group_id="com.microsoft.azure", 
                artifact_id="azure-eventhubs-spark_2.12",
                version="2.3.22"
            )], pypi_libraries=[], pythonwheel_libraries=[])
    assert isinstance(eventhub_destination.settings(), dict)
    assert eventhub_destination.pre_write_validation()
    assert eventhub_destination.post_write_validation()

def test_spark_eventhub_write_batch(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch("pyspark.sql.DataFrame.write", new_callable=mocker.Mock(return_value=mocker.Mock(format=mocker.Mock(return_value=mocker.Mock(options=mocker.Mock(return_value=mocker.Mock(save=mocker.Mock(return_value=None))))))))
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    eventhub_destination = SparkEventhubDestination(expected_df, {})
    actual = eventhub_destination.write_batch()
    assert actual is None

def test_spark_eventhub_write_stream(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch("pyspark.sql.DataFrame.writeStream", new_callable=mocker.Mock(return_value=mocker.Mock(format=mocker.Mock(return_value=mocker.Mock(options=mocker.Mock(return_value=mocker.Mock(start=mocker.Mock(return_value=TestStreamingQueryClass()))))))))
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    eventhub_destination = SparkEventhubDestination(expected_df, {})
    actual = eventhub_destination.write_stream()
    assert actual is None

def test_spark_eventhub_write_batch_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch("pyspark.sql.DataFrame.write", new_callable=mocker.Mock(return_value=mocker.Mock(format=mocker.Mock(return_value=mocker.Mock(options=mocker.Mock(return_value=mocker.Mock(save=mocker.Mock(side_effect=Exception))))))))
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    eventhub_destination = SparkEventhubDestination(expected_df, {})
    with pytest.raises(Exception):
        eventhub_destination.write_batch()

def test_spark_eventhub_write_stream_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch("pyspark.sql.DataFrame.writeStream", new_callable=mocker.Mock(return_value=mocker.Mock(format=mocker.Mock(return_value=mocker.Mock(options=mocker.Mock(return_value=mocker.Mock(start=mocker.Mock(side_effect=Exception))))))))
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    eventhub_destination = SparkEventhubDestination(expected_df, {})
    with pytest.raises(Exception):
        eventhub_destination.write_stream()
