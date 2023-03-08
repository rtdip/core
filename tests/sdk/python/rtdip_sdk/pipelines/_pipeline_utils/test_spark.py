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
from pyspark.sql import SparkSession
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import SparkClient
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import SPARK_TESTING_CONFIGURATION
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries

def test_get_spark_session():
    spark_client = SparkClient({**{"configuration_test1": "configuration_test_value1", "configuration_test2": "configuration_test_value2"}, **SPARK_TESTING_CONFIGURATION}, Libraries())
    spark = spark_client.spark_session
    assert isinstance(spark, SparkSession)
    assert spark.conf.get("configuration_test1") == "configuration_test_value1"
    assert spark.conf.get("configuration_test2") == "configuration_test_value2"

def test_get_spark_session_exception():
    with pytest.raises(Exception) as excinfo:
        spark_client = SparkClient({}, None)
        spark = spark_client.get_spark_session([], "test_get_spark_session_exception", "configuration_test1")
    assert str(excinfo.value) == 'not all arguments converted during string formatting' 