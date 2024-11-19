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

from src.sdk.python.rtdip_sdk.pipelines.logging.spark.runtime_log_collector import (RuntimeLogCollector)
from src.sdk.python.rtdip_sdk.pipelines.monitoring.spark.data_quality.identify_missing_data_interval import (
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




def test_logger_manager_basic_function():
    df = DataFrame()
    monitor = IdentifyMissingDataInterval(
        df=df,
        interval="10s",
        tolerance="500ms",
    )
    log_collector = RuntimeLogCollector()

    assert (monitor.logger_manager is log_collector.logger_manager)

def test_df_output(spark, caplog):
    log_collector = RuntimeLogCollector()
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
    log_collector._attach_dataframe_handler_to_loggers()

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()

    result_df = log_collector.get_logs_as_df()

    assert (result_df.shape[0] == 6)

def test_file_logging(spark, caplog):


    log_collector = RuntimeLogCollector()
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
    log_collector._attach_file_handler_to_loggers("logs.log", ".")

    with caplog.at_level(logging.INFO, logger="IdentifyMissingDataInterval"):
        monitor.check()

    with open("./logs.log", "r") as f:
        logs = f.readlines()

    assert (len(logs) == 6)
    if os.path.exists("./logs.log"):
        os.remove("./logs.log")