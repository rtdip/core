import pytest
from src.sdk.python.rtdip_sdk.monitoring.data_quality.great_expectations.python.great_expectations_data_quality import (
    GreatExpectationsDataQuality,
)
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


class TestGreatExpectations:
    @pytest.fixture(autouse=True)
    def setup_class(self, mocker):
        self.gx = GreatExpectations("context_root_dir", "df", "expectation_suite_name")
        self.mocker = mocker

    def test_create_expectations(self):
        mock_get_context = self.mocker.patch("great_expectations.gx.get_context")
        mock_context = self.mocker.MagicMock()
        mock_get_context.return_value = mock_context
        mock_context.add_or_update_expectation_suite.return_value = "suite"
        mock_context.get_validator.return_value = "validator"
        self.gx._create_batch_request = self.mocker.MagicMock(
            return_value="batch_request"
        )
        validator, suite = self.gx.create_expectations()
        assert validator == "validator"
        assert suite == "suite"

    def test_build_expectations(self):
        expectation_configuration = self.gx.build_expectations(
            "exception_type", "exception_dict", "meta_dict"
        )
        assert isinstance(expectation_configuration, ExpectationConfiguration)

    def test_add_expectations(self):
        mock_suite = self.mocker.MagicMock()
        mock_expectation_configuration = self.mocker.MagicMock()
        self.gx.add_expectations(mock_suite, mock_expectation_configuration)
        mock_suite.add_expectation_configuration.assert_called_once_with(
            expectation_configuration=mock_expectation_configuration
        )

    def test_remove_expectations(self):
        mock_suite = self.mocker.MagicMock()
        mock_expectation_configuration = self.mocker.MagicMock()
        self.gx.remove_expectations(mock_suite, mock_expectation_configuration)
        mock_suite.remove_expectation.assert_called_once_with(
            expectation_configuration=mock_expectation_configuration,
            match_type="domain",
            remove_multiple_matches=False,
        )

    def test_display_expectations(self):
        mock_suite = self.mocker.MagicMock()
        mock_suite.show_expectations_by_expectation_type.return_value = "expectation"
        expectation = self.gx.display_expectations(mock_suite)
        assert expectation == "expectation"

    def test_save_expectations(self):
        mock_validator = self.mocker.MagicMock()
        validator = self.gx.save_expectations(mock_validator)
        mock_validator.save_expectation_suite.assert_called_once_with(
            discard_failed_expectations=False
        )
        assert validator == mock_validator

    def test_check(self):
        mock_get_context = self.mocker.patch("great_expectations.gx.get_context")
        mock_context = self.mocker.MagicMock()
        mock_get_context.return_value = mock_context
        mock_checkpoint = self.mocker.patch("great_expectations.Checkpoint")
        mock_checkpoint_instance = self.mocker.MagicMock()
        mock_checkpoint.return_value = mock_checkpoint_instance
        mock_checkpoint_instance.run.return_value = "checkpoint_result"
        self.gx._create_batch_request = self.mocker.MagicMock(
            return_value="batch_request"
        )
        checkpoint_result = self.gx.check(
            "checkpoint_name", "run_name_template", ["action_list"]
        )
        assert checkpoint_result == "checkpoint_result"
