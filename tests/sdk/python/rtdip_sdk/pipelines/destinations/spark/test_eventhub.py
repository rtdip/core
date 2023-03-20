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
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery

def test_spark_eventhub_write_batch(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch("pyspark.sql.DataFrame.write", new_callable=mocker.Mock(return_value=mocker.Mock(format=mocker.Mock(return_value=mocker.Mock(options=mocker.Mock(return_value=mocker.Mock(save=mocker.Mock(return_value=None))))))))
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    eventhub_destination = SparkEventhubDestination({})
    actual = eventhub_destination.write_batch(expected_df)
    assert actual is None

def test_spark_eventhub_write_stream(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch("pyspark.sql.DataFrame.writeStream", new_callable=mocker.Mock(return_value=mocker.Mock(format=mocker.Mock(return_value=mocker.Mock(options=mocker.Mock(return_value=mocker.Mock(start=mocker.Mock(return_value=StreamingQuery))))))))
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    eventhub_destination = SparkEventhubDestination({})
    actual = eventhub_destination.write_stream(expected_df)
    assert eventhub_destination.pre_write_validation()
    assert actual is StreamingQuery
    assert eventhub_destination.post_write_validation()