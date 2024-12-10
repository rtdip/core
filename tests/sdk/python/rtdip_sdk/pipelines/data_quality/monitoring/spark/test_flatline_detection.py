import pytest
import os
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
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

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
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.0"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.0"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "Null"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    detector = FlatlineDetection(df, watch_columns=["Value"], tolerance_timespan=2)
    detector.check()

    expected_logs = [
        "Flatlining detected in column 'Value' at row: Row(TagName='A2PS64V0J.:ZUX09R', EventTime=datetime.datetime(2024, 1, 2, 7, 53, 11), Status='Good', Value=0.0, Value_flatline_flag=1, Value_group=1).",
        "Flatlining detected in column 'Value' at row: Row(TagName='A2PS64V0J.:ZUX09R', EventTime=datetime.datetime(2024, 1, 2, 11, 56, 42), Status='Good', Value=0.0, Value_flatline_flag=1, Value_group=1).",
        "Flatlining detected in column 'Value' at row: Row(TagName='A2PS64V0J.:ZUX09R', EventTime=datetime.datetime(2024, 1, 2, 16, 0, 12), Status='Good', Value=None, Value_flatline_flag=1, Value_group=1).",
    ]
    actual_logs = log_capture.getvalue().strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected in actual, f"Expected: '{expected}', got: '{actual}'"


def test_flatline_detection_with_tolerance(spark, log_capture):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.0"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.0"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "Null"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

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


def test_large_dataset(spark, log_capture):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    df = spark.read.option("header", "true").csv(file_path)

    print(df.count)
    assert df.count() > 0, "Dataframe was not loaded correct"

    detector = FlatlineDetection(df, watch_columns=["Value"], tolerance_timespan=2)
    detector.check()

    expected_logs = [
        "Flatlining detected in column 'Value' at row: Row(TagName='FLATLINE_TEST', EventTime=datetime.datetime(2024, 1, 2, 2, 35, 10, 511000), Status='Good', Value=0.0, Value_flatline_flag=1, Value_group=1).",
        "Flatlining detected in column 'Value' at row: Row(TagName='FLATLINE_TEST', EventTime=datetime.datetime(2024, 1, 2, 2, 49, 10, 408000), Status='Good', Value=0.0, Value_flatline_flag=1, Value_group=1).",
        "Flatlining detected in column 'Value' at row: Row(TagName='FLATLINE_TEST', EventTime=datetime.datetime(2024, 1, 2, 14, 57, 10, 372000), Status='Good', Value=0.0, Value_flatline_flag=1, Value_group=1).",
    ]
    actual_logs = log_capture.getvalue().strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected in actual, f"Expected: '{expected}', got: '{actual}'"
