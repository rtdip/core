import pytest
import os
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.logging.logger_manager import LoggerManager
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
    logger_manager = LoggerManager()
    logger = logger_manager.create_logger("IdentifyMissingDataInterval")

    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    yield log_stream
    logger.removeHandler(handler)
    handler.close()


def test_missing_intervals_with_given_interval_multiple_tags(spark, caplog):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:00.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:10.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:20.000", "Good", "0.129999995"),
            (
                "A2PS64V0J.:ZUX09R",
                "2024-01-02 00:00:36.000",
                "Good",
                "0.150000006",
            ),  # Missing interval (20s to 36s)
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:45.000", "Good", "0.340000004"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:55.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:05.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:15.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:25.000", "Good", "0.150000006"),
            (
                "A2PS64V0J.:ZUX09R",
                "2024-01-02 00:01:41.000",
                "Good",
                "0.340000004",
            ),  # Missing interval (25s to 41s)
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

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
        "Tag: A2PS64V0J.:ZUX09R Missing Interval from 2024-01-02 00:00:20 to 2024-01-02 00:00:36 Duration: 0h 0m 16s",
        "Tag: A2PS64V0J.:ZUX09R Missing Interval from 2024-01-02 00:01:25 to 2024-01-02 00:01:41 Duration: 0h 0m 16s",
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

    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:00.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:10.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:20.000", "Good", "0.129999995"),
            (
                "A2PS64V0J.:ZUX09R",
                "2024-01-02 00:00:36.000",
                "Good",
                "0.150000006",
            ),  # Missing interval (20s to 36s)
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:45.000", "Good", "0.340000004"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:55.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:05.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:15.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:25.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:30.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )
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
        "Tag: A2PS64V0J.:ZUX09R Missing Interval from 2024-01-02 00:00:20 to 2024-01-02 00:00:36 Duration: 0h 0m 16s",
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

    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:00.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:10.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:20.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:30.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:40.000", "Good", "0.340000004"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:50.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:00.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:10.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:20.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:01:30.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )
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
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )
    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="10seconds",  # should be '10s'
    )

    with pytest.raises(ValueError) as exc_info:
        with caplog.at_level(logging.ERROR, logger="IdentifyMissingDataInterval"):
            monitor.check()

    assert "Invalid time format: 10seconds" in str(exc_info.value)
    assert "Invalid time format: 10seconds" in caplog.text


def test_large_data_set(spark, caplog):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    df = spark.read.option("header", "true").csv(file_path)
    assert df.count() > 0, "Dataframe was not loaded correct"
    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="1s",
        tolerance="10ms",
    )
    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()
    expected_logs = [
        "Tag: MISSING_DATA Missing Interval from 2024-01-02 00:08:11 to 2024-01-02 00:08:13 Duration: 0h 0m 2s"
    ]
    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "INFO"
        and record.name == "IdentifyMissingDataInterval"
        and "MISSING_DATA" in record.message
    ]

    assert any(
        expected in actual for expected in expected_logs for actual in actual_logs
    ), "Expected logs not found in actual logs"
