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
import os

import pytest
from pyspark.sql import SparkSession
import numpy as np
from scipy.ndimage import gaussian_filter1d

from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.spark.gaussian_smoothing.gaussian_smoothing import (
    GaussianSmoothing
)


@pytest.fixture(scope="session")
def spark_session():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("GaussianSmoothingTest")
        .getOrCreate()
    )
    yield spark
    spark.stop()


def test_gaussian_smoothing_scipy(spark_session: SparkSession):
    """Test Gaussian smoothing using scipy implementation"""
    # Create test data
    df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"]
    )

    # Apply smoothing
    smoother = GaussianSmoothing(
        df=df,
        sigma=1.0,
        id_col="TagName",
        timestamp_col="EventTime",
        value_col="Value"
    )
    result_df = smoother.filter()

    result_df.show(

    )


def test_gaussian_smoothing(spark_session: SparkSession):
    # Create test data
    df = spark_session.createDataFrame(
        [
            ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", "0.119999997"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", "0.129999995"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 16:00:12.000", "Good", "0.150000006"),
            ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", "0.340000004"),
        ],
        ["TagName", "EventTime", "Status", "Value"]
    )

    # Apply smoothing
    smoother = GaussianSmoothing(
        df=df,
        sigma=1.0,
        id_col="TagName",
        timestamp_col="EventTime",
        value_col="Value"
    )
    result_df = smoother.filter()

    # Basic validations
    assert "smoothed_value" in result_df.columns
    assert result_df.count() == df.count()

    # Convert to pandas for value checks
    pdf = result_df.toPandas()
    assert all(pdf['smoothed_value'].notna())  # No null values


def test_interval_detection_large_data_set(spark_session: SparkSession):
    base_path = os.path.dirname(__file__)
    file_path = os.path.join(base_path, "../../test_data.csv")

    df = spark_session.read.option("header", "true").csv(file_path)

    # Apply smoothing
    smoother = GaussianSmoothing(
        df=df,
        sigma=1.0,
        id_col="TagName",
        timestamp_col="EventTime",
        value_col="Value"
    )

    actual_df = smoother.filter()
    actual_df.show()



