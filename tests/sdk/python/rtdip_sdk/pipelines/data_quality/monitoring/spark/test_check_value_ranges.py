import pytest
from pyspark.sql import SparkSession
from io import StringIO
import logging
import os


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
        ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "1"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "2"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "3"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "4"),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "5"),
        ("Tag2", "2024-01-02 03:49:45.000", "Good", "1"),
        ("Tag2", "2024-01-02 07:53:11.000", "Good", "2"),
        ("Tag2", "2024-01-02 11:56:42.000", "Good", "3"),
        ("Tag2", "2024-01-02 16:00:12.000", "Good", "4"),
        ("Tag2", "2024-01-02 20:03:46.000", "Good", "5"),
    ]
    return spark.createDataFrame(data, ["TagName", "EventTime", "Status", "Value"])


def test_basic(test_data, log_capture):
    tag_ranges = {
        "A2PS64V0J.:ZUX09R": {"min": 2, "max": 4, "inclusive_bounds": True},
        "Tag2": {"min": 1, "max": 5, "inclusive_bounds": False},
    }
    monitor = CheckValueRanges(test_data, tag_ranges)
    monitor.check()
    expected_logs = [
        # For temperature with inclusive_bounds='both'
        "Found 2 rows in 'Value' column for TagName 'A2PS64V0J.:ZUX09R' out of range.",
        f"Out of range row for TagName 'A2PS64V0J.:ZUX09R': Row(TagName='A2PS64V0J.:ZUX09R', EventTime=datetime.datetime(2024, 1, 2, 3, 49, 45), Status='Good', Value=1.0)",
        f"Out of range row for TagName 'A2PS64V0J.:ZUX09R': Row(TagName='A2PS64V0J.:ZUX09R', EventTime=datetime.datetime(2024, 1, 2, 20, 3, 46), Status='Good', Value=5.0)",
        f"Found 2 rows in 'Value' column for TagName 'Tag2' out of range.",
        f"Out of range row for TagName 'Tag2': Row(TagName='Tag2', EventTime=datetime.datetime(2024, 1, 2, 3, 49, 45), Status='Good', Value=1.0)",
        f"Out of range row for TagName 'Tag2': Row(TagName='Tag2', EventTime=datetime.datetime(2024, 1, 2, 20, 3, 46), Status='Good', Value=5.0)",
    ]
    log_contents = log_capture.getvalue()
    actual_logs = log_contents.strip().split("\n")
    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected == actual, f"Expected: '{expected}', got: '{actual}'"


def test_invalid_tag_name(test_data):
    tag_ranges = {
        "InvalidTagName": {"min": 0, "max": 100},
    }
    with pytest.raises(ValueError) as excinfo:
        monitor = CheckValueRanges(df=test_data, tag_ranges=tag_ranges)
        monitor.check()

    assert "TagName 'InvalidTagName' not found in DataFrame." in str(excinfo.value)


def test_no_min_or_max(test_data):
    tag_ranges = {
        "A2PS64V0J.:ZUX09R": {},  # Weder 'min' noch 'max' angegeben
    }
    with pytest.raises(ValueError) as excinfo:
        monitor = CheckValueRanges(df=test_data, tag_ranges=tag_ranges)
        monitor.check()
    assert (
        "TagName 'A2PS64V0J.:ZUX09R' must have at least 'min' or 'max' specified."
        in str(excinfo.value)
    )


def test_large_dataset(spark, log_capture):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    df = spark.read.option("header", "true").csv(file_path)
    assert df.count() > 0, "Dataframe was not loaded correct"

    tag_ranges = {
        "value_range": {"min": 2, "max": 4, "inclusive_bounds": True},
    }
    monitor = CheckValueRanges(df, tag_ranges)
    monitor.check()

    expected_logs = [
        "Found 2 rows in 'Value' column for TagName 'value_range' out of range.",
        f"Out of range row for TagName 'value_range': Row(TagName='value_range', EventTime=datetime.datetime(2024, 1, 2, 3, 49, 45), Status=' Good', Value=1.0)",
        f"Out of range row for TagName 'value_range': Row(TagName='value_range', EventTime=datetime.datetime(2024, 1, 2, 20, 3, 46), Status=' Good', Value=5.0)",
    ]
    actual_logs = log_capture.getvalue().strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"
    for expected, actual in zip(expected_logs, actual_logs):
        assert expected in actual, f"Expected: '{expected}', got: '{actual}'"
