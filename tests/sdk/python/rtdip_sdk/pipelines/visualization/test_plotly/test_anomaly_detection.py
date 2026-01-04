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

from datetime import datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.visualization.plotly.anomaly_detection import (
    AnomalyDetectionPlotInteractive,
)


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


@pytest.fixture(scope="session")
def spark():
    """
    Provide a SparkSession for tests.
    """
    return SparkSession.builder.appName("AnomalyDetectionPlotlyTests").getOrCreate()


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


def test_plotly_creates_figure_with_anomalies(spark: SparkSession):
    """A figure with time series and anomaly markers is created."""
    base = datetime(2024, 1, 1)

    ts_data = [(base + timedelta(seconds=i), float(i)) for i in range(10)]
    ad_data = [(base + timedelta(seconds=5), 5.0)]

    ts_df = spark.createDataFrame(ts_data, ["timestamp", "value"])
    ad_df = spark.createDataFrame(ad_data, ["timestamp", "value"])

    plot = AnomalyDetectionPlotInteractive(
        ts_data=ts_df,
        ad_data=ad_df,
        sensor_id="TEST_SENSOR",
    )

    fig = plot.plot()

    assert fig is not None
    assert len(fig.data) == 2  # line + anomaly
    assert fig.data[0].name == "value"
    assert fig.data[1].name == "anomaly"


def test_plotly_without_anomalies_creates_single_trace(spark: SparkSession):
    """If no anomalies are provided, only the time series is plotted."""
    base = datetime(2024, 1, 1)

    ts_data = [(base + timedelta(seconds=i), float(i)) for i in range(10)]

    ts_df = spark.createDataFrame(ts_data, ["timestamp", "value"])

    plot = AnomalyDetectionPlotInteractive(ts_data=ts_df)

    fig = plot.plot()

    assert fig is not None
    assert len(fig.data) == 1
    assert fig.data[0].name == "value"


def test_anomaly_hover_template_is_present(spark: SparkSession):
    """Anomaly markers expose timestamp and value via hover tooltip."""
    base = datetime(2024, 1, 1)

    ts_df = spark.createDataFrame(
        [(base, 1.0)],
        ["timestamp", "value"],
    )

    ad_df = spark.createDataFrame(
        [(base, 1.0)],
        ["timestamp", "value"],
    )

    plot = AnomalyDetectionPlotInteractive(
        ts_data=ts_df,
        ad_data=ad_df,
    )

    fig = plot.plot()

    anomaly_trace = fig.data[1]

    assert anomaly_trace.hovertemplate is not None
    assert "Timestamp" in anomaly_trace.hovertemplate
    assert "Value" in anomaly_trace.hovertemplate


def test_title_fallback_with_sensor_id(spark: SparkSession):
    """The title is derived from the sensor_id if no custom title is given."""
    base = datetime(2024, 1, 1)

    ts_df = spark.createDataFrame(
        [(base, 1.0)],
        ["timestamp", "value"],
    )

    plot = AnomalyDetectionPlotInteractive(
        ts_data=ts_df,
        sensor_id="SENSOR_X",
    )

    fig = plot.plot()

    assert "SENSOR_X" in fig.layout.title.text
