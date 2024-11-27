import pytest
from pyspark.sql import SparkSession
from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.spark.flatline_detection import (
    FlatlineDetection,
)

import logging
from io import StringIO


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("FlatlineDetectionTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def log_capture():
    log_stream = StringIO()
    logger = logging.getLogger("FlatlineDetection")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    yield log_stream
    logger.removeHandler(handler)
    handler.close()


def test_flatline_detection_no_flatlining(spark, log_capture):
    data = [
        (1, 5),
        (2, 6),
        (3, 7),
        (4, 8),
        (5, 9),
    ]
    columns = ["ID", "Value"]
    df = spark.createDataFrame(data, columns)

    detector = FlatlineDetection(df, watch_columns=["Value"], tolerance_timespan=2)
    detector.check()

    expected_logs = [
        "No flatlining detected in column 'Value'.",
    ]
    actual_logs = log_capture.getvalue().strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_flatline_detection_with_flatlining(spark, log_capture):
    data = [
        (1, 0),
        (2, 0),
        (3, 0),
        (4, 5),
        (5, 0),
    ]
    columns = ["ID", "Value"]
    df = spark.createDataFrame(data, columns)

    detector = FlatlineDetection(df, watch_columns=["Value"], tolerance_timespan=2)
    detector.check()

    expected_logs = [
        "Flatlining detected in column 'Value' at row: Row(ID=1, Value=0).",
        "Flatlining detected in column 'Value' at row: Row(ID=2, Value=0).",
        "Flatlining detected in column 'Value' at row: Row(ID=3, Value=0).",
    ]
    actual_logs = log_capture.getvalue().strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected in actual, f"Expected: '{expected}', got: '{actual}'"


def test_flatline_detection_multiple_columns(spark, log_capture):
    data = [
        (1, 0, None),
        (2, 0, None),
        (3, 0, None),
        (4, 1, 1),
        (5, 5, 2),
        (6, 0, None),
    ]
    columns = ["ID", "Value1", "Value2"]
    df = spark.createDataFrame(data, columns)

    detector = FlatlineDetection(
        df, watch_columns=["Value1", "Value2"], tolerance_timespan=2
    )
    detector.check()

    expected_logs = [
        "Flatlining detected in column 'Value1' at row: Row(ID=1, Value1=0, Value2=None).",
        "Flatlining detected in column 'Value1' at row: Row(ID=2, Value1=0, Value2=None).",
        "Flatlining detected in column 'Value1' at row: Row(ID=3, Value1=0, Value2=None).",
        "Flatlining detected in column 'Value2' at row: Row(ID=1, Value1=0, Value2=None).",
        "Flatlining detected in column 'Value2' at row: Row(ID=2, Value1=0, Value2=None).",
        "Flatlining detected in column 'Value2' at row: Row(ID=3, Value1=0, Value2=None).",
    ]
    actual_logs = log_capture.getvalue().strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected in actual, f"Expected: '{expected}', got: '{actual}'"


def test_flatline_detection_with_tolerance(spark, log_capture):
    data = [
        (1, 0),
        (2, 0),
        (3, 5),
        (4, 0),
        (5, 0),
        (6, 0),
    ]
    columns = ["ID", "Value"]
    df = spark.createDataFrame(data, columns)

    detector = FlatlineDetection(df, watch_columns=["Value"], tolerance_timespan=3)
    detector.check()

    expected_logs = [
        "No flatlining detected in column 'Value'.",
    ]
    actual_logs = log_capture.getvalue().strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected in actual, f"Expected: '{expected}', got: '{actual}'"
