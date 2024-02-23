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

from src.sdk.python.rtdip_sdk.monitoring.data_quality.pydeequ.spark.great_expectations_data_quality import (
    PyDeequDataQuality,
)
from src.sdk.python.rtdip_sdk.monitoring._monitoring_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    IntegerType,
)

from pydeequ.suggestions import ConstraintSuggestionRunner
from pydeequ.analyzers import AnalysisRunner, AnalyzerContext
from pydeequ.profiles import ColumnProfilerRunner

data2 = [
    ("James", "", "Smith", "36636", "M", 3000),
    ("Michael", "Rose", "", "40288", "M", 4000),
    ("Robert", "", "Williams", "42114", "M", 4000),
    ("Maria", "Anne", "Jones", "39192", "F", 4000),
    ("Jen", "Mary", "Brown", "", "F", -1),
]

schema = StructType(
    [
        StructField("firstname", StringType(), True),
        StructField("middlename", StringType(), True),
        StructField("lastname", StringType(), True),
        StructField("id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True),
    ]
)

test_df = SparkSession.createDataFrame(data=data2, schema=schema)


def test_profiles(spark_session: SparkSession):

    expected_result = ColumnProfilerRunner(spark_session).onData(test_df).run()

    actual_result = PyDeequDataQuality(spark_session, test_df).profiles()

    assert expected_result.collect() == actual_result.collect
    assert isinstance(actual_result, dict)


def test_analyse(spark_session: SparkSession):

    expected_analysis_result = (
        AnalysisRunner(spark_session)
        .onData(test_df)
        .addAnalyzer(Size())
        .addAnalyzer(Completeness())
        .addAnalyzer(ApproxCountDistinct())
        .addAnalyzer(CountDistinct())
        .addAnalyzer(Datatype())
        .addAnalyzer(Distinctness())
        .addAnalyzer(Entropy())
        .addAnalyzer(Mean())
        .addAnalyzer(Compliance())
        .addAnalyzer(Correlation())
        .addAnalyzer(Maxium())
        .addAnalyzer(Minimum())
        .addAnalyzer(MaxLength())
        .addAnalyzer(MinLength())
        .addAnalyzer(StandardDeviation())
        .addAnalyzer(Sum())
        .addAnalyzer(Uniqueness())
        .addAnalyzer(MutualInformation())
        .run()
    )

    expected_analysis_result_df = AnalyzerContext.successMetricsAsDataFrame(
        spark_session, expected_analysis_result
    )

    actual_analysis_result_df = PyDeequDataQuality(spark_session, test_df).analyse()

    assert expected_analysis_result_df.shape == actual_analysis_result_df.shape
    assert expected_analysis_result_df.schema == actual_analysis_result_df.schema
    assert expected_analysis_result_df.collect() == actual_analysis_result_df.collect()
    assert isinstance(actual_analysis_result_df, DataFrame)


def test_suggestions(spark_session: SparkSession):

    expected_suggestion_result = (
        ConstraintSuggestionRunner(spark_session)
        .onData(test_df)
        .addConstraintRule(DEFAULT())
        .run()
    )

    actual_suggestion_result = PyDeequDataQuality(spark_session, test_df).suggestions()

    assert expected_suggestion_result.collect() == actual_suggestion_result.collect()
    assert isinstance(actual_suggestion_result, dict)


def test_check(spark_session: SparkSession):

    df_checked_constraints, df_checked_constraints_failures = PyDeequDataQuality(
        spark_session, test_df
    ).check()

    assert isinstance(df_checked_constraints, DataFrame)
    assert isinstance(df_checked_constraints_failures, DataFrame)
