# Copyright 2024 RTDIP
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
import os

import pytest

from pandas import DataFrame
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.logging.logger_manager import LoggerManager
from src.sdk.python.rtdip_sdk.pipelines.logging.spark.runtime_log_collector import (
    RuntimeLogCollector,
)
from src.sdk.python.rtdip_sdk.pipelines.data_quality.monitoring.spark.identify_missing_data_interval import (
    IdentifyMissingDataInterval,
)

import logging


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("LogCollectionTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_logger_manager_basic_function(spark):
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:00.000", "Good", "0.129999995"),
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
    log_collector = RuntimeLogCollector(spark)

    assert monitor.logger_manager is log_collector.logger_manager


def test_df_output(spark, caplog):
    log_collector = RuntimeLogCollector(spark)
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:00.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:10.000", "Good", "0.119999997"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="10s",
        tolerance="500ms",
    )
    log_handler = log_collector._attach_dataframe_handler_to_logger(
        "IdentifyMissingDataInterval"
    )

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()

    result_df = log_handler.get_logs_as_df()

    assert result_df.count() == 4


def test_unique_dataframes(spark, caplog):
    log_collector = RuntimeLogCollector(spark)
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:00.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:10.000", "Good", "0.119999997"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )
    logger = LoggerManager().create_logger("Test_Logger")
    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="10s",
        tolerance="500ms",
    )
    log_handler_identify_missing_data_interval = (
        log_collector._attach_dataframe_handler_to_logger("IdentifyMissingDataInterval")
    )

    log_handler_test = log_collector._attach_dataframe_handler_to_logger("Test_Logger")

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()

    result_df = log_handler_identify_missing_data_interval.get_logs_as_df()
    result_df_test = log_handler_test.get_logs_as_df()

    assert result_df.count() != result_df_test.count()


def test_file_logging(spark, caplog):

    log_collector = RuntimeLogCollector(spark)
    df = spark.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:00.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 00:00:10.000", "Good", "0.119999997"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )
    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="10s",
        tolerance="500ms",
    )
    log_collector._attach_file_handler_to_loggers("logs.log", ".")

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()

    with open("./logs.log", "r") as f:
        logs = f.readlines()

    assert len(logs) == 4
    if os.path.exists("./logs.log"):
        os.remove("./logs.log")
