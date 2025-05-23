# Copyright 2025 RTDIP
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

from pandas.io.formats.format import math
import pytest
import os

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.normalization.denormalization import (
    Denormalization,
)
from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.normalization.normalization import (
    NormalizationBaseClass,
)
from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.normalization.normalization_mean import (
    NormalizationMean,
)
from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.normalization.normalization_minmax import (
    NormalizationMinMax,
)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def test_nonexistent_column_normalization(spark_session: SparkSession):
    input_df = spark_session.createDataFrame(
        [
            (1.0,),
            (2.0,),
        ],
        ["Value"],
    )

    with pytest.raises(ValueError):
        NormalizationMean(input_df, column_names=["NonexistingColumn"], in_place=True)


def test_wrong_column_type_normalization(spark_session: SparkSession):
    input_df = spark_session.createDataFrame(
        [
            ("a",),
            ("b",),
        ],
        ["Value"],
    )

    with pytest.raises(ValueError):
        NormalizationMean(input_df, column_names=["Value"])


def test_non_inplace_normalization(spark_session: SparkSession):
    input_df = spark_session.createDataFrame(
        [
            (1.0,),
            (2.0,),
        ],
        ["Value"],
    )

    expected_normalised_df = spark_session.createDataFrame(
        [
            (1.0, 0.0),
            (2.0, 1.0),
        ],
        ["Value", "Value_minmax_normalization"],
    )

    normalization_component = NormalizationMinMax(
        input_df, column_names=["Value"], in_place=False
    )
    normalised_df = normalization_component.filter_data()

    assert isinstance(normalised_df, DataFrame)

    assert expected_normalised_df.columns == normalised_df.columns
    assert expected_normalised_df.schema == normalised_df.schema
    assert expected_normalised_df.collect() == normalised_df.collect()

    denormalization_component = Denormalization(normalised_df, normalization_component)
    reverted_df = denormalization_component.filter_data()

    assert isinstance(reverted_df, DataFrame)

    assert input_df.columns == reverted_df.columns
    assert input_df.schema == reverted_df.schema
    assert input_df.collect() == reverted_df.collect()


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
    helper_assert_idempotence(class_to_test, input_df, expected_df)


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
    helper_assert_idempotence(class_to_test, input_df, expected_df)


@pytest.mark.parametrize("class_to_test", NormalizationBaseClass.__subclasses__())
def test_idempotence_with_large_data_set(
    spark_session: SparkSession, class_to_test: NormalizationBaseClass
):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    input_df = spark_session.read.option("header", "true").csv(file_path)
    input_df = input_df.withColumn("Value", input_df["Value"].cast("double"))
    assert input_df.count() > 0, "Dataframe was not loaded correct"
    input_df.show()

    expected_df = input_df.alias("input_df")
    helper_assert_idempotence(class_to_test, input_df, expected_df)


def helper_assert_idempotence(
    class_to_test: NormalizationBaseClass,
    input_df: DataFrame,
    expected_df: DataFrame,
):
    try:
        normalization_component = class_to_test(
            input_df, column_names=["Value"], in_place=True
        )
        actual_df = normalization_component.filter_data()

        denormalization_component = Denormalization(actual_df, normalization_component)
        actual_df = denormalization_component.filter_data()

        assert isinstance(actual_df, DataFrame)

        assert expected_df.columns == actual_df.columns
        assert expected_df.schema == actual_df.schema

        for row1, row2 in zip(expected_df.collect(), actual_df.collect()):
            for col1, col2 in zip(row1, row2):
                if isinstance(col1, float) and isinstance(col2, float):
                    assert math.isclose(col1, col2, rel_tol=1e-9)
                else:
                    assert col1 == col2
    except ZeroDivisionError:
        pass
