import pytest
from pyspark.sql import SparkSession
from io import StringIO
import logging

from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.spark.check_value_ranges import (
    CheckValueRanges,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("CheckValueRangesTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def log_capture():
    log_stream = StringIO()
    logger = logging.getLogger("CheckValueRanges")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    yield log_stream
    logger.removeHandler(handler)
    handler.close()


@pytest.fixture
def test_data(spark):
    data = [
        {"temperature": 50, "pressure": 100, "humidity": 50},
        {"temperature": 50, "pressure": 150, "humidity": 70},
        {"temperature": 70, "pressure": 200, "humidity": 90},
        {"temperature": 70, "pressure": 250, "humidity": 110},
        {"temperature": 90, "pressure": 300, "humidity": 130},
    ]
    return spark.createDataFrame(data)


def test_multiple_inclusive_options(test_data, log_capture):
    columns_ranges = {
        "temperature": {"min": 50, "max": 70, "inclusive": "both"},
        "pressure": {"min": 100, "max": 200, "inclusive": "left"},
        "humidity": {"min": 50, "max": 90, "inclusive": "right"},
    }
    monitor = CheckValueRanges(
        df=test_data, columns_ranges=columns_ranges, default_inclusive="neither"
    )
    monitor.check()

    expected_logs = [
        # For temperature with inclusive='both'
        "Found 1 rows in column 'temperature' out of range.",
        f"Out of range row in column 'temperature': {test_data.collect()[4]}",
        # For pressure with inclusive='left'
        "Found 3 rows in column 'pressure' out of range.",
        f"Out of range row in column 'pressure': {test_data.collect()[2]}",
        f"Out of range row in column 'pressure': {test_data.collect()[3]}",
        f"Out of range row in column 'pressure': {test_data.collect()[4]}",
        # For humidity with inclusive='right'
        "Found 3 rows in column 'humidity' out of range.",
        f"Out of range row in column 'humidity': {test_data.collect()[0]}",
        f"Out of range row in column 'humidity': {test_data.collect()[3]}",
        f"Out of range row in column 'humidity': {test_data.collect()[4]}",
    ]

    log_contents = log_capture.getvalue()
    actual_logs = log_contents.strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"

    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_inclusive_neither(test_data, log_capture):
    columns_ranges = {
        "temperature": {"min": 49, "max": 70},
    }
    monitor = CheckValueRanges(
        df=test_data, columns_ranges=columns_ranges, default_inclusive="neither"
    )
    monitor.check()

    expected_logs = [
        "Found 3 rows in column 'temperature' out of range.",
        f"Out of range row in column 'temperature': {test_data.collect()[2]}",
        f"Out of range row in column 'temperature': {test_data.collect()[3]}",
        f"Out of range row in column 'temperature': {test_data.collect()[4]}",
    ]

    log_contents = log_capture.getvalue()
    actual_logs = log_contents.strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_min_only(test_data, log_capture):
    columns_ranges = {
        "pressure": {"min": 150},
    }
    monitor = CheckValueRanges(df=test_data, columns_ranges=columns_ranges)
    monitor.check()

    expected_logs = [
        "Found 1 rows in column 'pressure' out of range.",
        f"Out of range row in column 'pressure': {test_data.collect()[0]}",
    ]

    log_contents = log_capture.getvalue()
    actual_logs = log_contents.strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_max_only(test_data, log_capture):
    columns_ranges = {
        "humidity": {"max": 90},
    }
    monitor = CheckValueRanges(df=test_data, columns_ranges=columns_ranges)
    monitor.check()

    expected_logs = [
        "Found 2 rows in column 'humidity' out of range.",
        f"Out of range row in column 'humidity': {test_data.collect()[3]}",
        f"Out of range row in column 'humidity': {test_data.collect()[4]}",
    ]

    log_contents = log_capture.getvalue()
    actual_logs = log_contents.strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_invalid_column(test_data):
    columns_ranges = {
        "invalid_column": {"min": 0, "max": 100},
    }
    with pytest.raises(ValueError) as excinfo:
        monitor = CheckValueRanges(df=test_data, columns_ranges=columns_ranges)
        monitor.check()
    assert "Column 'invalid_column' not found in DataFrame." in str(excinfo.value)


def test_invalid_inclusive(test_data):
    columns_ranges = {
        "temperature": {"min": 0, "max": 100},
    }
    with pytest.raises(ValueError) as excinfo:
        monitor = CheckValueRanges(
            df=test_data, columns_ranges=columns_ranges, default_inclusive="invalid"
        )
        monitor.check()
    assert (
        "Default inclusive parameter must be one of ['both', 'neither', 'left', 'right']."
        in str(excinfo.value)
    )


def test_no_min_or_max(test_data):
    columns_ranges = {
        "temperature": {},
    }
    with pytest.raises(ValueError) as excinfo:
        monitor = CheckValueRanges(df=test_data, columns_ranges=columns_ranges)
        monitor.check()
    assert "Column 'temperature' must have at least 'min' or 'max' specified." in str(
        excinfo.value
    )
