# Copyright 2022 RTDIP
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
from datetime import datetime

import pytest

from pyspark.sql import SparkSession
from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.data_quality.interval_filtering import (
    IntervalFiltering,
)


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()


def convert_to_datetime(date_time: str):
    return datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S.%f")


def test_interval_detection_easy(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    interval_filtering_wrangler = IntervalFiltering(
        spark_session, df, 1, "seconds", "EventTime"
    )
    actual_df = interval_filtering_wrangler.filter()

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_interval_detection_easy_unordered(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    interval_filtering_wrangler = IntervalFiltering(
        spark_session, df, 1, "seconds", "EventTime"
    )
    actual_df = interval_filtering_wrangler.filter()

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_interval_detection_milliseconds(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 20:03:46.020"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 20:03:46.030"),
        ],
        ["TagName", "Time"],
    )

    df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 20:03:46.020"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.025"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 20:03:46.030"),
            ("A2PS64V0J.:ZUasdX09R", "2024-01-02 20:03:46.035"),
        ],
        ["TagName", "Time"],
    )

    interval_filtering_wrangler = IntervalFiltering(
        spark_session, df, 10, "milliseconds", "Time"
    )
    actual_df = interval_filtering_wrangler.filter()

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_interval_detection_minutes(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 20:06:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 20:12:46.030"),
        ],
        ["TagName", "Time"],
    )

    df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 20:06:46.000"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:09:45.999"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 20:12:46.030"),
            ("A2PS64V0J.:ZUasdX09R", "2024-01-02 20:03:46.035"),
        ],
        ["TagName", "Time"],
    )

    interval_filtering_wrangler = IntervalFiltering(
        spark_session, df, 3, "minutes", "Time"
    )
    actual_df = interval_filtering_wrangler.filter()

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_interval_detection_hours(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:06:46.000"),
            ("A2PS64V0J.:ZUasdX09R", "2024-01-02 23:03:46.035"),
        ],
        ["TagName", "EventTime"],
    )

    df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:06:46.000"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 21:09:45.999"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:12:46.030"),
            ("A2PS64V0J.:ZUasdX09R", "2024-01-02 23:03:46.035"),
        ],
        ["TagName", "EventTime"],
    )

    interval_filtering_wrangler = IntervalFiltering(spark_session, df, 1, "hours")
    actual_df = interval_filtering_wrangler.filter()

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_interval_detection_days(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-03 21:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-04 21:12:46.030"),
            ("A2PS64V0J.:ZUasdX09R", "2028-01-01 23:03:46.035"),
        ],
        ["TagName", "EventTime"],
    )

    df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-03 21:03:46.000"),
            ("A2PS64V0J.:ZUX09R", "2024-01-04 21:03:45.999"),
            ("A2PS64asd.:ZUX09R", "2024-01-04 21:12:46.030"),
            ("A2PS64V0J.:ZUasdX09R", "2028-01-01 23:03:46.035"),
        ],
        ["TagName", "EventTime"],
    )

    interval_filtering_wrangler = IntervalFiltering(spark_session, df, 1, "days")
    actual_df = interval_filtering_wrangler.filter()

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_interval_detection_wrong_time_stamp_column_name(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:06:46.000"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 21:09:45.999"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:12:46.030"),
            ("A2PS64V0J.:ZUasdX09R", "2024-01-02 23:03:46.035"),
        ],
        ["TagName", "EventTime"],
    )

    interval_filtering_wrangler = IntervalFiltering(
        spark_session, df, 1, "hours", "Time"
    )

    with pytest.raises(ValueError):
        interval_filtering_wrangler.filter()


def test_interval_detection_wrong_interval_unit_pass(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:06:46.000"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 21:09:45.999"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:12:46.030"),
            ("A2PS64V0J.:ZUasdX09R", "2024-01-02 23:03:46.035"),
        ],
        ["TagName", "EventTime"],
    )

    interval_filtering_wrangler = IntervalFiltering(
        spark_session, df, 1, "years", "EventTime"
    )

    with pytest.raises(ValueError):
        interval_filtering_wrangler.filter()


def test_interval_detection_faulty_time_stamp(spark_session: SparkSession):
    df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", "2024-01-09-02 20:03:46.000"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:06:46.000"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 21:09:45.999"),
            ("A2PS64asd.:ZUX09R", "2024-01-02 21:12:46.030"),
            ("A2PS64V0J.:ZUasdX09R", "2024-01-02 23:03:46.035"),
        ],
        ["TagName", "EventTime"],
    )

    interval_filtering_wrangler = IntervalFiltering(
        spark_session, df, 1, "minutes", "EventTime"
    )

    with pytest.raises(ValueError):
        interval_filtering_wrangler.filter()


def test_interval_tolerance(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:47.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:50.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:52.000", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:46.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:47.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:50.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:51.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:52.000", "Good", "0.129999995"),
        ],
        ["TagName", "EventTime", "Status", "Value"],
    )

    interval_filtering_wrangler = IntervalFiltering(
        spark_session, df, 3, "seconds", "EventTime", 1
    )
    actual_df = interval_filtering_wrangler.filter()

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()


def test_interval_detection_date_time_columns(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", convert_to_datetime("2024-01-02 20:03:46.000")),
            ("A2PS64asd.:ZUX09R", convert_to_datetime("2024-01-02 21:06:46.000")),
            ("A2PS64V0J.:ZUasdX09R", convert_to_datetime("2024-01-02 23:03:46.035")),
        ],
        ["TagName", "EventTime"],
    )
    df = spark_session.createDataFrame(
        [
            ("A2PS64V0JR", convert_to_datetime("2024-01-02 20:03:46.000")),
            ("A2PS64asd.:ZUX09R", convert_to_datetime("2024-01-02 21:06:46.000")),
            ("A2PS64V0J.:ZUX09R", convert_to_datetime("2024-01-02 21:09:45.999")),
            ("A2PS64asd.:ZUX09R", convert_to_datetime("2024-01-02 21:12:46.030")),
            ("A2PS64V0J.:ZUasdX09R", convert_to_datetime("2024-01-02 23:03:46.035")),
        ],
        ["TagName", "EventTime"],
    )

    interval_filtering_wrangler = IntervalFiltering(spark_session, df, 1, "hours")
    actual_df = interval_filtering_wrangler.filter()

    assert expected_df.columns == actual_df.columns
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
