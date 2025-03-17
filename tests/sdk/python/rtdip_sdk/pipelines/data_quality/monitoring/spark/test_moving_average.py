# Copyright 2025 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import pytest
import os
from pyspark.sql import SparkSession
from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.spark.moving_average import (
    MovingAverage,
)
import logging
from io import StringIO


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("MovingAverageTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def log_capture():
    log_stream = StringIO()
    logger = logging.getLogger("MovingAverage")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(log_stream)
    formatter = logging.Formatter("%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    yield log_stream
    logger.removeHandler(handler)
    handler.close()


def test_moving_average_basic(spark, log_capture):
    df = spark.createDataFrame(
        [
            ("Tag1", "2024-01-02 03:49:45.000", "Good", 1.0),
            ("Tag1", "2024-01-02 07:53:11.000", "Good", 2.0),
            ("Tag1", "2024-01-02 11:56:42.000", "Good", 3.0),
            ("Tag1", "2024-01-02 16:00:12.000", "Good", 4.0),
            ("Tag1", "2024-01-02 20:03:46.000", "Good", 5.0),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    detector = MovingAverage(df, window_size=3)
    detector.check()

    expected_logs = [
        "Computing moving averages:",
        "Tag: Tag1, Time: 2024-01-02 03:49:45, Value: 1.0, Moving Avg: 1.0",
        "Tag: Tag1, Time: 2024-01-02 07:53:11, Value: 2.0, Moving Avg: 1.5",
        "Tag: Tag1, Time: 2024-01-02 11:56:42, Value: 3.0, Moving Avg: 2.0",
        "Tag: Tag1, Time: 2024-01-02 16:00:12, Value: 4.0, Moving Avg: 3.0",
        "Tag: Tag1, Time: 2024-01-02 20:03:46, Value: 5.0, Moving Avg: 4.0",
    ]

    actual_logs = log_capture.getvalue().strip().split("\n")

    assert len(expected_logs) == len(
        actual_logs
    ), f"Expected {len(expected_logs)} logs, got {len(actual_logs)}"

    for expected, actual in zip(expected_logs, actual_logs):
        assert expected in actual, f"Expected: '{expected}', got: '{actual}'"


def test_moving_average_invalid_window_size(spark):
    df = spark.createDataFrame(
        [
            ("Tag1", "2024-01-02 03:49:45.000", "Good", 1.0),
            ("Tag1", "2024-01-02 07:53:11.000", "Good", 2.0),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    with pytest.raises(ValueError, match="window_size must be a positive integer."):
        MovingAverage(df, window_size=-2)


def test_large_dataset(spark):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    df = spark.read.option("header", "true").csv(file_path)

    assert df.count() > 0, "DataFrame was nicht geladen."

    detector = MovingAverage(df, window_size=5)
    detector.check()
