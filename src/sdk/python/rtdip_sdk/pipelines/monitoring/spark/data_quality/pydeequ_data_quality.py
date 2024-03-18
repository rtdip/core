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

from pyspark.sql import DataFrame, SparkSession
from ...._pipeline_utils.models import Libraries, SystemType
from ...._pipeline_utils.constants import get_default_package

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite, VerificationResult
from pydeequ.suggestions import ConstraintSuggestionRunner
from pydeequ.analyzers import AnalysisRunner, AnalyzerContext
from pydeequ.profiles import ColumnProfilerRunner


class PyDeequDataQuality:
    """
    Base class for data quality checks, profiles and suggestions using PyDeequ.

        Example
        --------
        ```python
        #
        from src.sdk.python.rtdip_sdk.pipelines.monitoring.spark.data_quality.pydeequ_data_quality import PyDeequDataQuality
        from rtdip_sdk.pipelines.utilities import SparkSessionUtility
        import json

        # Not required if using Databricks
        spark = SparkSessionUtility(config={}).execute()

        df = example_df_from_spark

        PyDQ = PyDeequDataQuality(spark, df)

        #Run the Data Quality Profile
        profile = PyDQ.profiles()
        print(profile)

        #Run the Data Quality Analysis
        analysis = PyDQ.analyse()
        print(analysis)

        #Run the Data Quality Suggestions
        suggestions = PyDQ.suggestions()
        print(suggestions)

        #Run the Data Quality Checks based on the suggestions
        checks, failed_checks = PyDQ.check()

        print(checks)
        print(failed_checks)

        ```

    Parameters:
        spark (SparkSession): Spark Session instance.
        data (DataFrame): Dataframe containing the raw MISO data.
    """

    spark: SparkSession
    data: DataFrame

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
    ):
        self.spark = spark
        self.data = data

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def settings() -> dict:
        return {}

    @staticmethod
    def libraries():
        spark_libraries = Libraries()
        spark_libraries.add_maven_library(get_default_package("pydeequ"))
        return spark_libraries

    def profiles(self) -> list:
        result = ColumnProfilerRunner(self.spark).onData(self.data).run()
        return result

    def analyse(self) -> DataFrame:
        """
        Run the analysis on the data for all columns for the following metrics:
        Size, Completeness, ApproxCountDistinct, CountDistinct, Datatype, Distinctness, Entropy, Mean, Compliance, Correlation, Maxium, Minimum, MaxLength, MinLength, StandardDeviation, Sum, Uniqueness, MutualInformation

        Returns: Computed metrics as a DataFrame.

        """
        analysis_result = (
            AnalysisRunner(self.spark)
            .onData(self.data)
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

        analysis_result_df = AnalyzerContext.successMetricsAsDataFrame(
            self.spark, analysis_result
        )

        return analysis_result_df

    def suggestions(self):
        """
        Returns:  Data quality suggestions as a dictionary for the dataframe.

        """
        suggestion_result = (
            ConstraintSuggestionRunner(self.spark)
            .onData(self.data)
            .addConstraintRule(DEFAULT())
            .run()
        )
        return suggestion_result

    def check(self):
        """
        Inputs the data quality suggestions and performs the checks on the dataframe.


        Returns:
            df_checked_constraints (DataFrame): The dataframe containing the results of the data quality checks.
            df_checked_constraints_failures (DataFrame): The dataframe containing the results of the data quality checks that failed.
        """

        suggestion_result = self.suggestions()

        # Creating empty string to concatenate against
        pydeequ_validation_string = ""

        # Building string from suggestions
        for suggestion in suggestion_result["constraint_suggestions"]:
            pydeequ_validation_string = (
                pydeequ_validation_string + suggestion["code_for_constraint"]
            )

        # Initializing
        check = Check(
            spark_session=self.spark,
            level=CheckLevel.Warning,
            description="Data Quality Check",
        )

        # Building validation string of constraints to check
        pydeequ_validation_string_to_check = "check" + pydeequ_validation_string

        # Checking constraints
        checked_constraints = (
            VerificationSuite(self.spark)
            .onData(self.data)
            .addCheck(eval(pydeequ_validation_string_to_check))
            .run()
        )

        # Returning results as DataFrame
        df_checked_constraints = VerificationResult.checkResultsAsDataFrame(
            self.spark, checked_constraints
        )

        # Filtering for any failed data quality constraints
        df_checked_constraints_failures = df_checked_constraints.filter(
            df_checked_constraints.col("constraint_status") == "Failure"
        )

        return df_checked_constraints, df_checked_constraints_failures
