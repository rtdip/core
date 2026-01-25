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

"""Tests for matplotlib anomaly detection visualization components."""

import tempfile
import matplotlib.pyplot as plt
import pytest

from pathlib import Path

from matplotlib.figure import Figure

from src.sdk.python.rtdip_sdk.pipelines.visualization.matplotlib.anomaly_detection import (
    AnomalyDetectionPlot,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


@pytest.fixture
def spark_ts_data(spark_session):
    """Create sample time series data as PySpark DataFrame."""
    data = [
        (1, 10.0),
        (2, 12.0),
        (3, 10.5),
        (4, 11.0),
        (5, 30.0),
        (6, 10.2),
        (7, 9.8),
        (8, 10.1),
        (9, 10.3),
        (10, 10.0),
    ]
    columns = ["timestamp", "value"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def spark_anomaly_data(spark_session):
    """Create sample anomaly data as PySpark DataFrame."""
    data = [
        (5, 30.0),  # Anomalous value at timestamp 5
    ]
    columns = ["timestamp", "value"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def spark_ts_data_large(spark_session):
    """Create larger time series data with multiple anomalies."""
    data = [
        (1, 5.8),
        (2, 6.6),
        (3, 6.2),
        (4, 7.5),
        (5, 7.0),
        (6, 8.3),
        (7, 8.1),
        (8, 9.7),
        (9, 9.2),
        (10, 10.5),
        (11, 10.7),
        (12, 11.4),
        (13, 12.1),
        (14, 11.6),
        (15, 13.0),
        (16, 13.6),
        (17, 14.2),
        (18, 14.8),
        (19, 15.3),
        (20, 15.0),
        (21, 16.2),
        (22, 16.8),
        (23, 17.4),
        (24, 18.1),
        (25, 17.7),
        (26, 18.9),
        (27, 19.5),
        (28, 19.2),
        (29, 20.1),
        (30, 20.7),
        (31, 0.0),  # Anomaly
        (32, 21.5),
        (33, 22.0),
        (34, 22.9),
        (35, 23.4),
        (36, 30.0),  # Anomaly
        (37, 23.8),
        (38, 24.9),
        (39, 25.1),
        (40, 26.0),
        (41, 40.0),  # Anomaly
        (42, 26.5),
        (43, 27.4),
        (44, 28.0),
        (45, 28.8),
        (46, 29.1),
        (47, 29.8),
        (48, 30.5),
        (49, 31.0),
        (50, 31.6),
    ]
    columns = ["timestamp", "value"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture
def spark_anomaly_data_large(spark_session):
    """Create anomaly data for large dataset."""
    data = [
        (31, 0.0),
        (36, 30.0),
        (41, 40.0),
    ]
    columns = ["timestamp", "value"]
    return spark_session.createDataFrame(data, columns)


class TestAnomalyDetectionPlot:
    """Tests for AnomalyDetectionPlot class."""

    def test_init(self, spark_ts_data, spark_anomaly_data):
        """Test AnomalyDetectionPlot initialization."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
            sensor_id="SENSOR_001",
        )

        assert plot.ts_data is not None
        assert plot.ad_data is not None
        assert plot.sensor_id == "SENSOR_001"
        assert plot.figsize == (18, 6)
        assert plot.anomaly_color == "red"
        assert plot.ts_color == "steelblue"

    def test_init_with_custom_params(self, spark_ts_data, spark_anomaly_data):
        """Test initialization with custom parameters."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
            sensor_id="SENSOR_002",
            title="Custom Anomaly Plot",
            figsize=(20, 8),
            linewidth=2.0,
            anomaly_marker_size=100,
            anomaly_color="orange",
            ts_color="navy",
        )

        assert plot.sensor_id == "SENSOR_002"
        assert plot.title == "Custom Anomaly Plot"
        assert plot.figsize == (20, 8)
        assert plot.linewidth == 2.0
        assert plot.anomaly_marker_size == 100
        assert plot.anomaly_color == "orange"
        assert plot.ts_color == "navy"

    def test_system_type(self):
        """Test that system_type returns SystemType.PYTHON."""

        assert AnomalyDetectionPlot.system_type() == SystemType.PYTHON

    def test_libraries(self):
        """Test that libraries returns a Libraries instance with correct dependencies."""

        libraries = AnomalyDetectionPlot.libraries()
        assert isinstance(libraries, Libraries)

    def test_component_attributes(self, spark_ts_data, spark_anomaly_data):
        """Test that component attributes are correctly set."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
            sensor_id="SENSOR_001",
            figsize=(20, 8),
            anomaly_color="orange",
        )

        assert plot.figsize == (20, 8)
        assert plot.anomaly_color == "orange"
        assert plot.ts_color == "steelblue"
        assert plot.sensor_id == "SENSOR_001"
        assert plot.linewidth == 1.6
        assert plot.anomaly_marker_size == 70

    def test_plot_returns_figure(self, spark_ts_data, spark_anomaly_data):
        """Test that plot() returns a matplotlib Figure."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
            sensor_id="SENSOR_001",
        )

        fig = plot.plot()
        assert isinstance(fig, Figure)
        plt.close(fig)

    def test_plot_with_custom_title(self, spark_ts_data, spark_anomaly_data):
        """Test plot with custom title."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
            title="My Custom Anomaly Detection",
        )

        fig = plot.plot()
        assert isinstance(fig, Figure)

        # Verify title is set
        ax = fig.axes[0]
        assert ax.get_title() == "My Custom Anomaly Detection"
        plt.close(fig)

    def test_plot_without_anomalies(self, spark_ts_data, spark_session):
        """Test plotting time series without any anomalies."""

        # declare schema for empty anomalies DataFrame
        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType

        schema = StructType(
            [
                StructField("timestamp", IntegerType(), True),
                StructField("value", DoubleType(), True),
            ]
        )
        empty_anomalies = spark_session.createDataFrame([], schema=schema)

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=empty_anomalies,
            sensor_id="SENSOR_001",
        )

        fig = plot.plot()
        assert isinstance(fig, Figure)
        plt.close(fig)

    def test_plot_large_dataset(self, spark_ts_data_large, spark_anomaly_data_large):
        """Test plotting with larger dataset and multiple anomalies."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data_large,
            ad_data=spark_anomaly_data_large,
            sensor_id="SENSOR_BIG",
        )

        fig = plot.plot()
        assert isinstance(fig, Figure)

        ax = fig.axes[0]
        assert len(ax.lines) >= 1
        plt.close(fig)

    def test_plot_with_ax(self, spark_ts_data, spark_anomaly_data):
        """Test plotting on existing matplotlib axis."""

        fig, ax = plt.subplots(figsize=(10, 5))

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data, ad_data=spark_anomaly_data, ax=ax
        )

        result_fig = plot.plot()
        assert result_fig == fig
        plt.close(fig)

    def test_save(self, spark_ts_data, spark_anomaly_data):
        """Test saving plot to file."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
            sensor_id="SENSOR_001",
        )

        plot.plot()

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_anomaly_detection.png"
            saved_path = plot.save(filepath)
            assert saved_path.exists()
            assert saved_path.suffix == ".png"

    def test_save_different_formats(self, spark_ts_data, spark_anomaly_data):
        """Test saving plot in different formats."""
        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
        )

        plot.plot()

        with tempfile.TemporaryDirectory() as tmpdir:
            # Test PNG
            png_path = Path(tmpdir) / "test.png"
            plot.save(png_path)
            assert png_path.exists()

            # Test PDF
            pdf_path = Path(tmpdir) / "test.pdf"
            plot.save(pdf_path)
            assert pdf_path.exists()

            # Test SVG
            svg_path = Path(tmpdir) / "test.svg"
            plot.save(svg_path)
            assert svg_path.exists()

    def test_save_with_custom_dpi(self, spark_ts_data, spark_anomaly_data):
        """Test saving plot with custom DPI."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
        )

        plot.plot()

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_high_dpi.png"
            plot.save(filepath, dpi=300)
            assert filepath.exists()

    def test_validate_data_missing_columns(self, spark_session):
        """Test that validation raises error for missing columns."""

        bad_data = spark_session.createDataFrame(
            [(1, 10.0), (2, 12.0)], ["time", "val"]
        )
        anomaly_data = spark_session.createDataFrame(
            [(1, 10.0)], ["timestamp", "value"]
        )

        with pytest.raises(ValueError, match="must contain columns"):
            AnomalyDetectionPlot(ts_data=bad_data, ad_data=anomaly_data)

    def test_validate_anomaly_data_missing_columns(self, spark_ts_data, spark_session):
        """Test that validation raises error for missing columns in anomaly data."""

        bad_anomaly_data = spark_session.createDataFrame([(1, 10.0)], ["time", "val"])

        with pytest.raises(ValueError, match="must contain columns"):
            AnomalyDetectionPlot(ts_data=spark_ts_data, ad_data=bad_anomaly_data)

    def test_data_sorting(self, spark_session):
        """Test that plot handles unsorted data correctly."""

        unsorted_data = spark_session.createDataFrame(
            [(5, 10.0), (1, 5.0), (3, 7.0), (2, 6.0), (4, 9.0)],
            ["timestamp", "value"],
        )
        anomaly_data = spark_session.createDataFrame([(3, 7.0)], ["timestamp", "value"])

        plot = AnomalyDetectionPlot(
            ts_data=unsorted_data, ad_data=anomaly_data, sensor_id="SENSOR_001"
        )

        fig = plot.plot()
        assert isinstance(fig, Figure)

        assert not plot.ts_data["timestamp"].is_monotonic_increasing
        plt.close(fig)

    def test_anomaly_detection_title_format(self, spark_ts_data, spark_anomaly_data):
        """Test that title includes anomaly count when sensor_id is provided."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
            sensor_id="SENSOR_001",
        )

        fig = plot.plot()
        ax = fig.axes[0]
        title = ax.get_title()

        assert "SENSOR_001" in title
        assert "1" in title
        plt.close(fig)

    def test_plot_axes_labels(self, spark_ts_data, spark_anomaly_data):
        """Test that plot has correct axis labels."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
        )

        fig = plot.plot()
        ax = fig.axes[0]

        assert ax.get_xlabel() == "timestamp"
        assert ax.get_ylabel() == "value"
        plt.close(fig)

    def test_plot_legend(self, spark_ts_data, spark_anomaly_data):
        """Test that plot has a legend."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
        )

        fig = plot.plot()
        ax = fig.axes[0]

        legend = ax.get_legend()
        assert legend is not None
        plt.close(fig)

    def test_multiple_plots_same_data(self, spark_ts_data, spark_anomaly_data):
        """Test creating multiple plots from the same component."""

        plot = AnomalyDetectionPlot(
            ts_data=spark_ts_data,
            ad_data=spark_anomaly_data,
        )

        fig1 = plot.plot()
        fig2 = plot.plot()

        assert isinstance(fig1, Figure)
        assert isinstance(fig2, Figure)

        plt.close(fig1)
        plt.close(fig2)
