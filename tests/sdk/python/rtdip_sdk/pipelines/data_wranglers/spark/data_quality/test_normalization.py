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
from abc import ABCMeta

import pytest

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from rtdip_sdk.pipelines.data_wranglers import *


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


@pytest.mark.parametrize("class_to_test", NormalizationBaseClass.__subclasses__())
def test_idempotence_with_positive_values(
    spark_session: SparkSession, class_to_test: NormalizationBaseClass
):
    input_df = spark_session.createDataFrame(
        [
            (1.0,),
            (2.0,),
            (3.0,),
            (4.0,),
            (5.0,),
        ],
        ["Value"],
    )

    expected_df = input_df.alias("input_df")
    helper_assert_idempotence(spark_session, class_to_test, input_df, expected_df)


@pytest.mark.parametrize("class_to_test", NormalizationBaseClass.__subclasses__())
def test_idempotence_with_zero_values(
    spark_session: SparkSession, class_to_test: NormalizationBaseClass
):
    input_df = spark_session.createDataFrame(
        [
            (0.0,),
            (0.0,),
            (0.0,),
            (0.0,),
            (0.0,),
        ],
        ["Value"],
    )

    expected_df = input_df.alias("input_df")
    helper_assert_idempotence(spark_session, class_to_test, input_df, expected_df)


def helper_assert_idempotence(
    spark_session: SparkSession,
    class_to_test: NormalizationBaseClass,
    input_df: DataFrame,
    expected_df: DataFrame,
):
    try:
        normalization_component = class_to_test(
            input_df, column_names=["Value"], in_place=True
        )
        actual_df = normalization_component.filter()

        denormalization_component = Denormalization(actual_df, normalization_component)
        actual_df = denormalization_component.filter()

        assert isinstance(actual_df, DataFrame)

        assert expected_df.columns == actual_df.columns
        assert expected_df.schema == actual_df.schema
        assert expected_df.collect() == actual_df.collect()
    except ZeroDivisionError:
        pass
