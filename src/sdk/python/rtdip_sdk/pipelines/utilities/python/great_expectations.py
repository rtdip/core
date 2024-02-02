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
from great_expectations.checkpoint import (
    Checkpoint,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


# Create a new context


class GreatExpectations:
    """ """

    def __init__(
        self,
        context_root_dir: str,
        df: str,
        expectation_suite_name: str,
        GXConfig: dict,
    ) -> None:
        self.context_root_dir = context_root_dir
        self.df = df
        self.expectation_suite_name = expectation_suite_name
        self.GXConfig = GXConfig

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

    # Create a new context

    def create_context(self):
        context = gx.get_context(context_root_dir=self.context_root_dir)
        return context

    # Create a batch request from a dataframe

    def create_batch_request(self):
        batch_request = self.df.build_batch_request()

        return batch_request

    # Create Expectations

    def create_expectations(self, context, batch_request):
        suite = context.add_or_update_expectation_suite(
            expectation_suite_name=self.expectation_suite_name
        )
        validator = context.get_validator(
            batch_request=batch_request,
            expectation_suite_name=self.expectation_suite_name,
        )
        return validator, suite

    def build_expectations(self, exception_type, exception_dict, meta_dict):
        expectation_configuration = ExpectationConfiguration(
            expectation_type=exception_type, kwargs=exception_dict, meta=meta_dict
        )

        return expectation_configuration

    def add_expectations(self, suite, expectation_configuration):
        # Add the Expectation to the suite
        suite.add_expectation_configuration(
            expectation_configuration=expectation_configuration
        )

    def remove_expectations(self, suite, expectation_configuration):
        suite.remove_expectation(
            expectation_configuration,
            match_type="domain",
            remove_multiple_matches=False,
        )

    def display_expectations(self, suite):
        expectation = suite.show_expectations_by_expectation_type()
        return expectation

    def add_expectations_no_null(self, validator, column):
        validator.expect_column_values_to_not_be_null(column=column)
        return validator

    def add_expectations_value_between(self, validator, column, min, max):
        validator.expect_column_values_to_be_between(
            column=column, min_value=min, max_value=max
        )
        return validator

    def add_expectations_value_in_set(self, validator, column, set):
        validator.expect_column_values_to_be_in_set(column=column, value_set=set)
        return validator

    def add_expectations_value_unique(self, validator, column):
        validator.expect_column_values_to_be_unique(column=column)
        return validator

    def add_expectations_value_type(self, validator, column, type):
        validator.expect_column_values_to_be_of_type(column=column, type_=type)
        return validator

    expect_column_chisquare_test_p_value_to_be_greater_than.py
    expect_column_distinct_values_to_be_in_set.py
    expect_column_distinct_values_to_contain_set.py
    expect_column_distinct_values_to_equal_set.py
    expect_column_kl_divergence_to_be_less_than.py
    expect_column_max_to_be_between.py
    expect_column_mean_to_be_between.py
    expect_column_median_to_be_between.py
    expect_column_min_to_be_between.py
    expect_column_most_common_value_to_be_in_set.py
    expect_column_pair_cramers_phi_value_to_be_less_than.py
    expect_column_pair_values_a_to_be_greater_than_b.py
    expect_column_pair_values_to_be_equal.py
    expect_column_pair_values_to_be_in_set.py
    expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than.py
    expect_column_proportion_of_unique_values_to_be_between.py
    expect_column_quantile_values_to_be_between.py
    expect_column_stdev_to_be_between.py
    expect_column_sum_to_be_between.py
    expect_column_to_exist.py
    expect_column_unique_value_count_to_be_between.py
    expect_column_value_lengths_to_be_between.py
    expect_column_value_lengths_to_equal.py
    expect_column_value_z_scores_to_be_less_than.py
    expect_column_values_to_be_between.py
    expect_column_values_to_be_dateutil_parseable.py
    expect_column_values_to_be_decreasing.py
    expect_column_values_to_be_in_set.py
    expect_column_values_to_be_in_type_list.py
    expect_column_values_to_be_increasing.py
    expect_column_values_to_be_json_parseable.py
    expect_column_values_to_be_null.py
    expect_column_values_to_be_of_type.py
    expect_column_values_to_be_unique.py
    expect_column_values_to_match_json_schema.py
    expect_column_values_to_match_like_pattern.py
    expect_column_values_to_match_like_pattern_list.py
    expect_column_values_to_match_regex.py
    expect_column_values_to_match_regex_list.py
    expect_column_values_to_match_strftime_format.py
    expect_column_values_to_not_be_in_set.py
    expect_column_values_to_not_be_null.py
    expect_column_values_to_not_match_like_pattern.py
    expect_column_values_to_not_match_like_pattern_list.py
    expect_column_values_to_not_match_regex.py
    expect_column_values_to_not_match_regex_list.py
    expect_compound_columns_to_be_unique.py
    expect_multicolumn_sum_to_equal.py
    expect_multicolumn_values_to_be_unique.py
    expect_select_column_values_to_be_unique_within_record.py
    expect_table_column_count_to_be_between.py
    expect_table_column_count_to_equal.py
    expect_table_columns_to_match_ordered_list.py
    expect_table_columns_to_match_set.py
    expect_table_row_count_to_be_between.py
    expect_table_row_count_to_equal.py
    expect_table_row_count_to_equal_other_table.py

    def save_expectations(self, validator):
        validator.save_expectation_suite(discard_failed_expectations=False)
        return validator

    # Validate your data

    def add_or_update_checkpoint(self, context, batch_request, checkpoint_name):
        checkpoint = Checkpoint(
            name=checkpoint_name,
            run_name_template="%Y%m%d-%H%M%S-my-run-name-template",
            data_context=context,
            batch_request=batch_request,
            expectation_suite_name=self.expectation_suite_name,
            action_list=[
                {
                    "name": "store_validation_result",
                    "action": {"class_name": "StoreValidationResultAction"},
                },
                {
                    "name": "update_data_docs",
                    "action": {"class_name": "UpdateDataDocsAction"},
                },
            ],
        )

        context.add_or_update_checkpoint(checkpoint=checkpoint)
        checkpoint_result = checkpoint.run()
        return checkpoint_result
