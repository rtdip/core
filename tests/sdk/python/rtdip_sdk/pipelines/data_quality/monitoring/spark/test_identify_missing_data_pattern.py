# tests/test_identify_missing_data_pattern.py

import pytest
import logging
import os

from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.spark.identify_missing_data_pattern import (
    IdentifyMissingDataPattern,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("IdentifyMissingDataPatternTest")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")  # Unterdrücke WARN-Messages
    yield spark
    spark.stop()


def test_no_missing_patterns(spark, caplog):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:00", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:13", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:49", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:01:00", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:01:13", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:01:49", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )
    patterns = [{"second": 0}, {"second": 13}, {"second": 49}]
    monitor = IdentifyMissingDataPattern(
        df=df, patterns=patterns, frequency="minutely", tolerance="1s"
    )

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataPattern"):
        monitor.check()

    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "INFO" and record.name == "IdentifyMissingDataPattern"
    ]
    assert "Using tolerance: 1000.0 ms (1.0 seconds)" in actual_logs
    assert "Identified 0 missing patterns." in actual_logs
    assert "No missing patterns detected." in actual_logs


def test_some_missing_patterns(spark, caplog):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:00", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:13", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:49", "Good", "0.129999995"),
            (
                "A2PS64V0J.:ZUX09R",
                "2024-02-11 00:01:05",
                "Good",
                "0.129999995",
            ),  # Nothing matches in minute 1
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:01:17", "Good", "0.119999997"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )
    patterns = [{"second": 0}, {"second": 13}, {"second": 49}]
    monitor = IdentifyMissingDataPattern(
        df=df, patterns=patterns, frequency="minutely", tolerance="1s"
    )

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataPattern"):
        monitor.check()

    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "INFO" and record.name == "IdentifyMissingDataPattern"
    ]
    assert "Using tolerance: 1000.0 ms (1.0 seconds)" in actual_logs
    assert "Identified 2 missing patterns." in actual_logs
    assert "Detected Missing Patterns:" in actual_logs
    assert "Missing Pattern at 2024-02-11 00:01:00.000" in actual_logs
    assert "Missing Pattern at 2024-02-11 00:01:13.000" in actual_logs


def test_all_missing_patterns(spark, caplog):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:05", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:17", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:29", "Good", "0.129999995"),
            (
                "A2PS64V0J.:ZUX09R",
                "2024-02-11 00:01:05",
                "Good",
                "0.129999995",
            ),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:01:17", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:01:29", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    patterns = [{"second": 0}, {"second": 13}, {"second": 49}]
    monitor = IdentifyMissingDataPattern(
        df=df, patterns=patterns, frequency="minutely", tolerance="1s"
    )

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataPattern"):
        monitor.check()

    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "INFO" and record.name == "IdentifyMissingDataPattern"
    ]
    assert "Using tolerance: 1000.0 ms (1.0 seconds)" in actual_logs
    assert "Identified 5 missing patterns." in actual_logs
    assert "Detected Missing Patterns:" in actual_logs
    missing_patterns = [
        "Missing Pattern at 2024-02-11 00:00:00.000",
        "Missing Pattern at 2024-02-11 00:00:13.000",
        "Missing Pattern at 2024-02-11 00:00:49.000",
        "Missing Pattern at 2024-02-11 00:01:00.000",
        "Missing Pattern at 2024-02-11 00:01:13.000",
    ]
    for pattern in missing_patterns:
        assert pattern in actual_logs


def test_invalid_patterns(spark, caplog):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:01:49", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    patterns = [
        {"minute": 0},  # Invalid for 'minutely' frequency
        {"second": 13},
        {"second": 49},
    ]
    monitor = IdentifyMissingDataPattern(
        df=df, patterns=patterns, frequency="minutely", tolerance="1s"
    )

    with pytest.raises(ValueError) as exc_info, caplog.at_level(
        logging.ERROR, logger="IdentifyMissingDataPattern"
    ):
        monitor.check()

    assert "Each pattern must have a 'second' key for 'minutely' frequency." in str(
        exc_info.value
    )


def test_invalid_tolerance_format(spark, caplog):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:01:49", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )
    patterns = [{"second": 0}, {"second": 13}, {"second": 49}]
    monitor = IdentifyMissingDataPattern(
        df=df, patterns=patterns, frequency="minutely", tolerance="1minute"
    )

    with pytest.raises(ValueError) as exc_info, caplog.at_level(
        logging.ERROR, logger="IdentifyMissingDataPattern"
    ):
        monitor.check()

    assert "Invalid tolerance format: 1minute" in str(exc_info.value)
    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "ERROR" and record.name == "IdentifyMissingDataPattern"
    ]
    assert "Invalid tolerance format: 1minute" in actual_logs


def test_hourly_patterns_with_microseconds(spark, caplog):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:00:00.200", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 00:59:59.800", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-02-11 01:00:30.500", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    patterns = [
        {"minute": 0, "second": 0, "millisecond": 0},
        {"minute": 30, "second": 30, "millisecond": 500},
    ]
    monitor = IdentifyMissingDataPattern(
        df=df, patterns=patterns, frequency="hourly", tolerance="500ms"
    )

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataPattern"):
        monitor.check()

    actual_logs = [
        record.message
        for record in caplog.records
        if record.levelname == "INFO" and record.name == "IdentifyMissingDataPattern"
    ]
    assert "Using tolerance: 500.0 ms (0.5 seconds)" in actual_logs
    assert "Identified 1 missing patterns." in actual_logs
    assert "Detected Missing Patterns:" in actual_logs
    assert "Missing Pattern at 2024-02-11 00:30:30.500" in actual_logs


def test_large_data_set(spark):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    df = spark.read.option("header", "true").csv(file_path)
    assert df.count() > 0, "Dataframe was not loaded correct"
    patterns = [{"second": 0}, {"second": 13}, {"second": 49}]
    monitor = IdentifyMissingDataPattern(
        df=df, patterns=patterns, frequency="minutely", tolerance="1s"
    )
    monitor.check()
