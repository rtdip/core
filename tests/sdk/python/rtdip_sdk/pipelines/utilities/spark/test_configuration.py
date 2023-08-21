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

from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.configuration import (
    SparkConfigurationUtility,
)
from pyspark.sql import SparkSession


def test_spark_configuration(spark_session: SparkSession):
    spark_config_utility = SparkConfigurationUtility(
        spark=spark_session, config={"config1": "test1", "config2": "test2"}
    )

    result = spark_config_utility.execute()
    assert result
    assert "test1" == spark_session.conf.get("config1")
    assert "test2" == spark_session.conf.get("config2")
