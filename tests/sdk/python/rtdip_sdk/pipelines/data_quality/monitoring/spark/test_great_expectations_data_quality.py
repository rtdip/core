from pytest_mock import MockerFixture
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.spark.great_expectations_data_quality import (
    GreatExpectationsDataQuality,
)

gx = GreatExpectationsDataQuality(
    spark=SparkSession,
    context_root_dir="context_root_dir/test/",
    df="test_df",
    expectation_suite_name="expectation_suite_name",
    df_datasource_name="my_spark_in_memory_datasource",
    df_asset_name="df_asset_name",
)


def test_create_expectations(mocker: MockerFixture):
    mock_context = mocker.MagicMock()
    gx._create_context = mocker.MagicMock(return_value=mock_context)
    mock_batch_request = mocker.MagicMock()
    gx._create_batch_request = mocker.MagicMock(return_value=mock_batch_request)
    mock_suite = mocker.MagicMock()
    mock_context.add_or_update_expectation_suite.return_value = mock_suite
    mock_validator = mocker.MagicMock()
    mock_context.get_validator.return_value = mock_validator

    validator, suite = gx.create_expectations()

    gx._create_context.assert_called_once()
    gx._create_batch_request.assert_called_once()
    mock_context.add_or_update_expectation_suite.assert_called_once_with(
        expectation_suite_name=gx.expectation_suite_name
    )
    mock_context.get_validator.assert_called_once_with(
        batch_request=mock_batch_request,
        expectation_suite_name=gx.expectation_suite_name,
    )
    assert validator == mock_validator
    assert suite == mock_suite


def test_build_expectations():
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
    expectation_configuration = gx.build_expectations(
        expectation_type, exception_dict, meta_dict
    )
    assert (
        expectation_configuration.expectation_type
        == "expect_column_values_to_not_be_null"
    )
    assert expectation_configuration.kwargs == {
        "column": "user_id",
        "mostly": 0.75,
    }
    assert expectation_configuration.meta == {
        "notes": {
            "format": "markdown",
            "content": "Some clever comment about this expectation. **Markdown** `Supported`",
        }
    }
    assert isinstance(expectation_type, str)
    assert isinstance(exception_dict, dict)
    assert isinstance(meta_dict, dict)


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
        remove_multiple_matches=True,
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
    checkpoint_name = "checkpoint_name"
    run_name_template = "run_name_template"
    action_list = ["action_list"]

    mock_context = mocker.MagicMock()
    gx._create_context = mocker.MagicMock(return_value=mock_context)
    mock_batch_request = mocker.MagicMock()
    gx._create_batch_request = mocker.MagicMock(return_value=mock_batch_request)

    assert isinstance(checkpoint_name, str)
    assert isinstance(run_name_template, str)
    assert isinstance(action_list, list)
