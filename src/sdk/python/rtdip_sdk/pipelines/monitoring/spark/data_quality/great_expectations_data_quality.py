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

import great_expectations as gx
from pyspark.sql import DataFrame, SparkSession
from great_expectations.checkpoint import (
    Checkpoint,
)
from great_expectations.expectations.expectation import (
    ExpectationConfiguration,
)


# Create a new context
class GreatExpectationsDataQuality:
    """
    Data Quality Monitoring using Great Expectations allowing you to create and check your data quality expectations.

    Example
    --------
    ```python
    #
    from src.sdk.python.rtdip_sdk.monitoring.data_quality.great_expectations.python.great_expectations_data_quality import  GreatExpectationsDataQuality
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility
    import json

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    df = example_df_from_spark
    context_root_dir = "great_expectations_dir",
    expectation_suite_name = "great_expectations_suite_name"

    expectation_type = "expect_column_values_to_not_be_null"
    exception_dict = {
        "column": "user_id",
        "mostly": 0.75,
    }
    meta_dict = {
        "notes": {
            "format": "markdown",
            "content": "Some clever comment about this expectation. **Markdown** `Supported`",
        }
    }

    #Configure the Great Expectations Data Quality

    GX = GreatExpectationsDataQuality(spark, context_root_dir, df, expectation_suite_name)

    validator, suite = GX.create_expectations(spark, context_root_dir, df, expectation_suite_name)

    expectation_configuration = GX.build_expectations(
        exception_type, exception_dict, meta_dict
    )

    GX.add_expectations(suite, expectation_configuration)

    GX.save_expectations(validator)

    GX.display_expectations(suite)

    #Run the Data Quality Check by Validating your data against set expecations in the suite

    checkpoint_name = "checkpoint_name"
    run_name_template = "run_name_template"
    action_list = [
        {
            "name": "store_validation_result",
            "action": {"class_name": "StoreValidationResultAction"},
        },
        {"name": "update_data_docs", "action": {"class_name": "UpdateDataDocsAction"}},
    ]

    checkpoint_result = GX.check(checkpoint_name, run_name_template, action_list)

    print(checkpoint_result)

    ```

    Parameters:
        df (DataFrame): Dataframe containing the raw data.
        context_root_dir (str): The root directory of the Great Expectations project.
        expectation_suite_name (str): The name of the expectation suite to be created.
    """

    def __init__(
        self,
        context_root_dir: str,
        df: DataFrame,
        expectation_suite_name: str,
    ) -> None:
        self.context_root_dir = context_root_dir
        self.df = df
        self.expectation_suite_name = expectation_suite_name

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
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    # Create a new context
    def _create_context(self):
        """
        Create a new context
        Returns: context
        """
        context = gx.get_context(context_root_dir=self.context_root_dir)
        return context

    # Create a batch request from a dataframe
    def _create_batch_request(self):
        """
        Create a batch request from a dataframe
        Returns: batch_request
        """
        batch_request = (self.df).build_batch_request()
        return batch_request

    # Create Expectations

    def create_expectations(self):
        context = self._create_context()
        batch_request = self._create_batch_request()

        suite = context.add_or_update_expectation_suite(
            expectation_suite_name=self.expectation_suite_name
        )
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=self.expectation_suite_name,
        )
        return validator, suite

    def build_expectations(
        self, exception_type: str, exception_dict: dict, meta_dict: dict
    ):
        expectation_configuration = ExpectationConfiguration(
            expectation_type=exception_type, kwargs=exception_dict, meta=meta_dict
        )
        return expectation_configuration

    def add_expectations(self, suite, expectation_configuration):
        suite.add_expectation_configuration(
            expectation_configuration=expectation_configuration
        )

    def remove_expectations(
        self, suite, expectation_configuration, remove_multiple_matches=False
    ):
        suite.remove_expectation(
            expectation_configuration=expectation_configuration,
            match_type="domain",
            remove_multiple_matches=remove_multiple_matches,
        )

    def display_expectations(self, suite):
        expectation = suite.show_expectations_by_expectation_type()
        return expectation

    def save_expectations(self, validator):
        validator.save_expectation_suite(discard_failed_expectations=False)
        return validator

    # Validate your data

    def check(
        self,
        checkpoint_name,
        run_name_template: str,
        action_list: list,
    ):
        """
        Validate your data against set expecations in the suite
        Args:
            checkpoint_name (str): The name of the checkpoint.
            run_name_template (str): The name of the run.
            action_list (list): The list of actions to be performed.
         Returns: checkpoint_result(dict)"""
        context = self._create_context()
        batch_request = self._create_batch_request()

        checkpoint = Checkpoint(
            name=checkpoint_name,
            run_name_template=run_name_template,
            data_context=context,
            batch_request=batch_request,
            expectation_suite_name=self.expectation_suite_name,
            action_list=action_list,
        )
        context.add_or_update_checkpoint(checkpoint=checkpoint)
        checkpoint_result = checkpoint.run()
        return checkpoint_result
