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

import pytest
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.dimensionality_reduction import (
    DimensionalityReduction,
)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


@pytest.fixture
def test_data(spark_session):
    normal_distribution = [
        0.30832997,
        0.22166579,
        -1.68713693,
        1.41243689,
        1.25282623,
        -0.70494665,
        0.52186887,
        -0.34352648,
        -1.38233527,
        -0.76870644,
        1.72735928,
        -0.14838714,
        -0.76086769,
        1.81330706,
        -1.84541331,
        -1.05816002,
        0.86864253,
        -2.47756826,
        0.19112086,
        -0.72390124,
    ]

    noise = [
        2.39757601,
        0.40913959,
        0.40281196,
        0.43624341,
        0.57281305,
        0.15978893,
        0.09098515,
        0.18199072,
        2.9758837,
        1.38059478,
        1.55032586,
        0.88507288,
        2.13327,
        2.21896827,
        0.61288938,
        0.17535961,
        1.83386377,
        1.08476656,
        1.86311249,
        0.44964528,
    ]

    data_with_noise = [
        (normal_distribution[i], normal_distribution[i] + noise[i])
        for i in range(len(normal_distribution))
    ]

    identical_data = [
        (normal_distribution[i], normal_distribution[i])
        for i in range(len(normal_distribution))
    ]

    return [
        spark_session.createDataFrame(data_with_noise, ["Value1", "Value2"]),
        spark_session.createDataFrame(identical_data, ["Value1", "Value2"]),
    ]


def test_with_correlated_data(spark_session, test_data):
    identical_data = test_data[1]

    dimensionality_reduction = DimensionalityReduction(
        identical_data, ["Value1", "Value2"]
    )
    result_df = dimensionality_reduction.filter()

    assert (
        result_df.count() == identical_data.count()
    ), "Row count does not match expected result"
    assert "Value1" in result_df.columns, "Value1 should be in the DataFrame"
    assert "Value2" not in result_df.columns, "Value2 should have been removed"


def test_with_uncorrelated_data(spark_session, test_data):
    uncorrelated_data = test_data[0]

    dimensionality_reduction = DimensionalityReduction(
        uncorrelated_data, ["Value1", "Value2"]
    )
    result_df = dimensionality_reduction.filter()

    assert (
        result_df.count() == uncorrelated_data.count()
    ), "Row count does not match expected result"
    assert "Value1" in result_df.columns, "Value1 should be in the DataFrame"
    assert "Value2" in result_df.columns, "Value2 should be in the DataFrame"
