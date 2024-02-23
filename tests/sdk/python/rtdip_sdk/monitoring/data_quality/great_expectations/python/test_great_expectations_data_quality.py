import pytest
from pytest_mock import MockerFixture
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

from src.sdk.python.rtdip_sdk.monitoring.data_quality.great_expectations.python.great_expectations_data_quality import (
    GreatExpectationsDataQuality,
)

gx = GreatExpectationsDataQuality(
    "context_root_dir", "test_df", "expectation_suite_name"
)


def test_create_expectations(mocker: MockerFixture):

    validator, suite = gx.create_expectations()
    gx._create_batch_request = mocker.patch(return_value="batch_request")
    gx._create_context = mocker.patch(return_value="context")
    gx._create_batch_request = mocker.MagicMock(return_value="batch_request")
    gx._create_context = mocker.MagicMock(return_value="context")
    assert validator == "validator"
    assert suite == "suite"


def test_build_expectations():
    expectation_configuration = gx.build_expectations(
        "exception_type", exception_dict, meta_dict
    )
    assert isinstance(expectation_configuration, ExpectationConfiguration)


def test_add_expectations(mocker: MockerFixture):
    mock_suite = mocker.MagicMock()
    mock_expectation_configuration = mocker.MagicMock()
    gx.add_expectations(mock_suite, mock_expectation_configuration)
    mock_suite.add_expectation_configuration.assert_called_once_with(
        expectation_configuration=mock_expectation_configuration
    )


def test_remove_expectations(mocker: MockerFixture):
    mock_suite = mocker.MagicMock()
    mock_expectation_configuration = mocker.MagicMock()
    gx.remove_expectations(mock_suite, mock_expectation_configuration)
    mock_suite.remove_expectation.assert_called_once_with(
        expectation_configuration=mock_expectation_configuration,
        match_type="domain",
        remove_multiple_matches=False,
    )


def test_display_expectations(mocker: MockerFixture):
    mock_suite = mocker.MagicMock()
    mock_suite.show_expectations_by_expectation_type.return_value = "expectation"
    expectation = gx.display_expectations(mock_suite)
    assert expectation == "expectation"


def test_save_expectations(mocker: MockerFixture):
    mock_validator = mocker.MagicMock()
    validator = gx.save_expectations(mock_validator)
    mock_validator.save_expectation_suite.assert_called_once_with(
        discard_failed_expectations=False
    )
    assert validator == mock_validator


def test_check(mocker: MockerFixture):
    gx._create_batch_request = mocker.patch(return_value="batch_request")
    gx._create_context = mocker.patch(return_value="context")
    mock_checkpoint = mocker.patch("great_expectations.Checkpoint")
    mock_checkpoint_instance = mocker.MagicMock()
    mock_checkpoint.return_value = mock_checkpoint_instance
    mock_checkpoint_instance.run.return_value = "checkpoint_result"
    gx._create_batch_request = mocker.MagicMock(return_value="batch_request")
    checkpoint_result = gx.check(
        "checkpoint_name", "run_name_template", ["action_list"]
    )
    assert checkpoint_result == "checkpoint_result"
