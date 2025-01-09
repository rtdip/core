import pytest
import os
from pyspark.sql import SparkSession
from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.flatline_filter import (
    FlatlineFilter,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("FlatlineDetectionTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_flatline_filter_no_flatlining(spark):
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

    detector = FlatlineFilter(df, watch_columns=["Value"], tolerance_timespan=2)
    result = detector.filter()

    assert sorted(result.collect()) == sorted(df.collect())


def test_flatline_detection_with_flatlining(spark):
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

    detector = FlatlineFilter(df, watch_columns=["Value"], tolerance_timespan=2)
    result = detector.filter()

    rows_to_remove = [
        {
            "TagName": "A2PS64V0J.:ZUX09R",
            "EventTime": "2024-01-02 07:53:11.000",
            "Status": "Good",
            "Value": "0.0",
        },
        {
            "TagName": "A2PS64V0J.:ZUX09R",
            "EventTime": "2024-01-02 07:53:11.000",
            "Status": "Good",
            "Value": "0.0",
        },
        {
            "TagName": "A2PS64V0J.:ZUX09R",
            "EventTime": "2024-01-02 11:56:42.000",
            "Status": "Good",
            "Value": "0.0",
        },
        {
            "TagName": "A2PS64V0J.:ZUX09R",
            "EventTime": "2024-01-02 16:00:12.000",
            "Status": "Good",
            "Value": "None",
        },
    ]
    rows_to_remove_df = spark.createDataFrame(rows_to_remove)
    expected_df = df.subtract(rows_to_remove_df)
    assert sorted(result.collect()) == sorted(expected_df.collect())


def test_large_dataset(spark):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")
    df = spark.read.option("header", "true").csv(file_path)

    assert df.count() > 0, "Dataframe was not loaded correctly"

    detector = FlatlineFilter(df, watch_columns=["Value"], tolerance_timespan=2)
    result = detector.filter()

    rows_to_remove = [
        {
            "TagName": "FLATLINE_TEST",
            "EventTime": "2024-01-02 02:35:10.511000",
            "Status": "Good",
            "Value": "0.0",
        },
        {
            "TagName": "FLATLINE_TEST",
            "EventTime": "2024-01-02 02:49:10.408000",
            "Status": "Good",
            "Value": "0.0",
        },
        {
            "TagName": "FLATLINE_TEST",
            "EventTime": "2024-01-02 14:57:10.372000",
            "Status": "Good",
            "Value": "0.0",
        },
    ]
    rows_to_remove_df = spark.createDataFrame(rows_to_remove)

    expected_df = df.subtract(rows_to_remove_df)

    assert sorted(result.collect()) == sorted(expected_df.collect())
