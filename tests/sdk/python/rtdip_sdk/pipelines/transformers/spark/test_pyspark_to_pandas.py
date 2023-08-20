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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.pyspark_to_pandas import (
    PySparkToPandasTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

import pandas as pd
from pandas import DataFrame
from pyspark.sql import SparkSession


def test_pyspark_to_pandas(spark_session: SparkSession):
    expected_df = pd.DataFrame([[1, "a"], [2, "b"]], columns=["col1", "col2"])

    df = spark_session.createDataFrame(
        [
            (1, "a"),
            (2, "b"),
        ],
        ["col1", "col2"],
    )

    pyspark_conversion_transformer = PySparkToPandasTransformer(df)
    actual_df = pyspark_conversion_transformer.transform()

    assert pyspark_conversion_transformer.system_type().value == 2
    assert pyspark_conversion_transformer.libraries() == Libraries()
    assert isinstance(pyspark_conversion_transformer.settings(), dict)

    assert isinstance(actual_df, DataFrame)

    assert expected_df.columns.equals(actual_df.columns)
    assert expected_df.dtypes.equals(actual_df.dtypes)
    assert expected_df.equals(actual_df)
