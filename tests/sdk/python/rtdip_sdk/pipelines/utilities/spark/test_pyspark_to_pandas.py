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
from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.pyspark_to_pandas import PySparkToPandasDFUtility
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pandas.core.frame import DataFrame
from pyspark.sql import SparkSession

def test_pyspark_to_pandas(spark_session: SparkSession):
    df = spark_session.createDataFrame(
    [
        (1, "a"),
        (2, "b"),
    ],
    ["col1", "col2"]
    )

    pyspark_conversion_utility = PySparkToPandasDFUtility(spark_session, df)
    result = pyspark_conversion_utility.execute()
    
    assert result
    assert isinstance(result, DataFrame)