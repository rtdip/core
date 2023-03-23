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
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub import SparkEventhubSource
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
import json
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

def test_spark_eventhub_read_batch(spark_session: SparkSession):
    connection_string = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test;EntityPath=test"
    eventhub_configuration = {
        "eventhubs.connectionString": connection_string, 
        "eventhubs.consumerGroup": "$Default",
        "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
    }
    eventhub_source = SparkEventhubSource(spark_session, eventhub_configuration)
    assert eventhub_source.pre_read_validation()
    df = eventhub_source.read_batch()
    assert isinstance(df, DataFrame)
    assert eventhub_source.post_read_validation(df)

def test_spark_eventhub_read_stream(spark_session: SparkSession):
    connection_string = "Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test;EntityPath=test"
    eventhub_configuration = {
        "eventhubs.connectionString": connection_string, 
        "eventhubs.consumerGroup": "$Default",
        "eventhubs.startingPosition": json.dumps({"offset": "0", "seqNo": -1, "enqueuedTime": None, "isInclusive": True})
    }
    eventhub_source = SparkEventhubSource(spark_session, eventhub_configuration)
    assert eventhub_source.pre_read_validation()
    df = eventhub_source.read_stream()
    assert isinstance(df, DataFrame)
    assert eventhub_source.post_read_validation(df)

def test_spark_eventhub_read_stream_2(spark_session: SparkSession, mocker: MockerFixture):
    eventhub_source = SparkEventhubSource(spark_session, {})
    expected_df = spark_session.createDataFrame([{"a": "x"}])
    mocker.patch.object(eventhub_source, "spark", new_callable=mocker.PropertyMock(return_value=mocker.Mock(readStream=mocker.Mock(format=mocker.Mock(return_value=mocker.Mock(options=mocker.Mock(return_value=mocker.Mock(load=mocker.Mock(return_value=expected_df)))))))))
    assert eventhub_source.pre_read_validation()
    df = eventhub_source.read_stream()
    assert isinstance(df, DataFrame) 
    assert eventhub_source.post_read_validation(df)