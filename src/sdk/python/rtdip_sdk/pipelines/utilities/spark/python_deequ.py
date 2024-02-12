from pyspark.sql import DataFrame, SparkSession

from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.suggestions import *
from pydeequ.analyzers import *
from pydeequ.profiles import *




class PythonDeequPipeline():
    """ """

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
            SystemType (Environment): Requires PYTHON
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_pypi_library(get_default_package("azure_adls_gen_2"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}
    
    def profiles(self):
        result = ColumnProfilerRunner(self.spark).onData(self.data).run()
        return result
    
    def analyse(self):
        analysisResult = (
            AnalysisRunner(self.spark)
            .onData(self.data)
            .addAnalyzer(Size())
            .addAnalyzer(Completeness())
            .addAnalyzer(ApproxCountDistinct())
            .addAnalyzer(Mean())
            .addAnalyzer(Compliance())
            .addAnalyzer(Correlation())
            .addAnalyzer(Correlation())
            .run()
        )
        analysisResult_df = AnalyzerContext.successMetricsAsDataFrame(self.spark, analysisResult)

        return analysisResult_df
    
    def suggestions(self):
        suggestionResult = (
            ConstraintSuggestionRunner(self.spark).onData(self.data).addConstraintRule(DEFAULT()).run()
        )
        return suggestionResult
    
    def perform_check_suggestions(self, suggestionResult):
    
    # Creating empty string to concatenate against
        pydeequ_validation_string = ""

        # Building string from suggestions
        for suggestion in suggestionResult['constraint_suggestions']:
            pydeequ_validation_string = pydeequ_validation_string + suggestion["code_for_constraint"]

        # Initializing
        check = \
            Check(spark_session=self.spark,
                level=CheckLevel.Warning,
                description="Data Quality Check")

        # Building validation string of constraints to check
        pydeequ_validation_string_to_check = "check" + pydeequ_validation_string

        # Checking constraints
        checked_constraints = \
            (VerificationSuite(spark)
            .onData(df)
            .addCheck(eval(pydeequ_validation_string_to_check))
            .run())

        # Returning results as DataFrame
        df_checked_constraints = \
            (VerificationResult
            .checkResultsAsDataFrame(spark, checked_constraints))

        logger.info(
            df_checked_constraints.show(n=df_checked_constraints.count(),
                                        truncate=False)
        )

        # Filtering for any failed data quality constraints
        df_checked_constraints_failures = \
            (df_checked_constraints
            .filter(F.col("constraint_status") == "Failure"))

        # If any data quality check fails, raise exception
        if df_checked_constraints_failures.count() > 0:
            logger.info(
                df_checked_constraints_failures.show(n=df_checked_constraints_failures.count(),
                                                    truncate=False)
    Share
Improve this answer
Follow
edited Feb 23, 2023 at 3:04

  
# Printing string validation string
# If desired, edit this string to control what data quality validations are performed
print(pydeequ_validation_string)


suggestionResult = (
    ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT()).run()
)

for sugg in suggestionResult["constraint_suggestions"]:
    print(f"Constraint suggestion for '{sugg['column_name']}': {sugg['description']}")
    print(f"The corresponding Python code is: {sugg['code_for_constraint']}\n")

suggestionResult = (
    ConstraintSuggestionRunner(spark).onData(df).addConstraintRule(DEFAULT()).run()
)

print(json.dumps(suggestionResult, indent=2))


from pydeequ.profiles import *

result = ColumnProfilerRunner(spark).onData(df).run()
print(result)

check = Check(spark, CheckLevel.Warning, "Amazon Electronic Products Reviews")

checkResult = (
    VerificationSuite(spark)
    .onData(df)
    .addCheck(
        check.hasSize(lambda x: x >= 3000000)
        .hasMin("star_rating", lambda x: x == 1.0)
        .hasMax("star_rating", lambda x: x == 5.0)
        .isComplete("review_id")
        .isUnique("review_id")
        .isComplete("marketplace")
        .isContainedIn("marketplace", ["US", "UK", "DE", "JP", "FR"])
        .isNonNegative("year")
    )
    .run()
)

print(f"Verification Run Status: {checkResult.status}")
checkResult_df = VerificationResult.checkResultsAsDataFrame(spark, checkResult)
checkResult_df.show()
