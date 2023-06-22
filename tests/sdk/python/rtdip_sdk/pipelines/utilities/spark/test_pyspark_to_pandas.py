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
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
import pandas as pd
from pandas import DataFrame
from pyspark.sql import SparkSession

def test_conversion_set_up(spark_session: SparkSession):
    df = spark_session.createDataFrame([(1, "a"), (2, "b"),], ["col1", "col2"])
    pyspark_conversion_utility = PySparkToPandasDFUtility(spark_session, df)

    assert pyspark_conversion_utility.system_type().value == 2
    assert pyspark_conversion_utility.libraries() == Libraries()
    assert isinstance(pyspark_conversion_utility.settings(), dict)

def test_pyspark_to_pandas(spark_session: SparkSession):
    expected_df = pd.DataFrame([[1, 'a'], [2, 'b']], columns=["col1", "col2"])

    df = spark_session.createDataFrame([(1, "a"), (2, "b"),], ["col1", "col2"])

    pyspark_conversion_utility = PySparkToPandasDFUtility(spark_session, df)
    result = pyspark_conversion_utility.execute()
    
    assert isinstance(result, DataFrame)
    assert expected_df.equals(result)
    assert expected_df.columns.equals(result.columns)