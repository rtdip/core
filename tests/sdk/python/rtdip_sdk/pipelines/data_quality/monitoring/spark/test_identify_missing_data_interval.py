import pytest
from pyspark.sql import SparkSession
from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.spark.identify_missing_data_interval import (
    IdentifyMissingDataInterval,
)

import logging
from io import StringIO


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("IdentifyMissingDataIntervalTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def log_capture():
    log_stream = StringIO()
    logger = logging.getLogger("IdentifyMissingDataInterval")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    yield log_stream
    logger.removeHandler(handler)
    handler.close()


def test_missing_intervals_with_given_interval(spark, caplog):
    data = [
        (1, "2024-02-11 00:00:00.000"),
        (2, "2024-02-11 00:00:10.000"),
        (3, "2024-02-11 00:00:20.000"),
        (4, "2024-02-11 00:00:36.000"),  # Missing interval (20s to 36s)
        (5, "2024-02-11 00:00:45.000"),
        (6, "2024-02-11 00:00:55.000"),
        (7, "2024-02-11 00:01:05.000"),
        (8, "2024-02-11 00:01:15.000"),
        (9, "2024-02-11 00:01:25.000"),
        (10, "2024-02-11 00:01:41.000"),  # Missing interval (25s to 41s)
    ]
    columns = ["Index", "EventTime"]
    df = spark.createDataFrame(data, schema=columns)
    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="10s",
        tolerance="500ms",
    )

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()
    expected_logs = [
        "Using provided expected interval: 10000.0 ms",
        "Using provided tolerance: 500.0 ms",
        "Maximum acceptable interval with tolerance: 10500.0 ms",
        "Detected Missing Intervals:",
        "Missing Interval from 2024-02-11 00:00:20 to 2024-02-11 00:00:36 Duration: 0h 0m 16s",
        "Missing Interval from 2024-02-11 00:01:25 to 2024-02-11 00:01:41 Duration: 0h 0m 16s",
    ]
    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "INFO" and record.name == "IdentifyMissingDataInterval"
    ]

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)} "
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_missing_intervals_with_calculated_interval(spark, caplog):
    data = [
        (1, "2024-02-11 00:00:00.000"),
        (2, "2024-02-11 00:00:10.000"),
        (3, "2024-02-11 00:00:20.000"),
        (4, "2024-02-11 00:00:36.000"),  # Missing interval (20s to 36s)
        (5, "2024-02-11 00:00:45.000"),
        (6, "2024-02-11 00:00:55.000"),
        (7, "2024-02-11 00:01:05.000"),
        (8, "2024-02-11 00:01:15.000"),
        (9, "2024-02-11 00:01:25.000"),
        (10, "2024-02-11 00:01:30.000"),
    ]
    columns = ["Index", "EventTime"]
    df = spark.createDataFrame(data, schema=columns)
    monitor = IdentifyMissingDataInterval(
        df=df,
    )

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()
    expected_logs = [
        "Using median of time differences as expected interval: 10000.0 ms",
        "Calculated tolerance: 10.0 ms (MAD-based)",
        "Maximum acceptable interval with tolerance: 10010.0 ms",
        "Detected Missing Intervals:",
        "Missing Interval from 2024-02-11 00:00:20 to 2024-02-11 00:00:36 Duration: 0h 0m 16s",
    ]
    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "INFO" and record.name == "IdentifyMissingDataInterval"
    ]

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)} "
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_no_missing_intervals(spark, caplog):
    data = [
        (1, "2024-02-11 00:00:00.000"),
        (2, "2024-02-11 00:00:10.000"),
        (3, "2024-02-11 00:00:20.000"),
        (4, "2024-02-11 00:00:30.000"),
        (5, "2024-02-11 00:00:40.000"),
        (6, "2024-02-11 00:00:50.000"),
        (7, "2024-02-11 00:01:00.000"),
        (8, "2024-02-11 00:01:10.000"),
        (9, "2024-02-11 00:01:20.000"),
        (10, "2024-02-11 00:01:30.000"),
    ]
    columns = ["Index", "EventTime"]
    df = spark.createDataFrame(data, schema=columns)
    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="10s",
        tolerance="5s",
    )

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()
    expected_logs = [
        "Using provided expected interval: 10000.0 ms",
        "Using provided tolerance: 5000.0 ms",
        "Maximum acceptable interval with tolerance: 15000.0 ms",
        "No missing intervals detected.",
    ]
    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "INFO" and record.name == "IdentifyMissingDataInterval"
    ]

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)} "
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_invalid_timedelta_format(spark, caplog):
    data = [
        (1, "2024-02-11 00:00:00.000"),
        (2, "2024-02-11 00:00:10.000"),
    ]
    columns = ["Index", "EventTime"]
    df = spark.createDataFrame(data, schema=columns)
    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="10seconds",  # should be '10s'
    )

    with pytest.raises(ValueError) as exc_info:
        with caplog.at_level(logging.ERROR, logger="IdentifyMissingDataInterval"):
            monitor.check()

    assert "Invalid time format: 10seconds" in str(exc_info.value)
    assert "Invalid time format: 10seconds" in caplog.text
