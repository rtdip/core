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

"""Tests for matplotlib decomposition visualization components."""

import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from src.sdk.python.rtdip_sdk.pipelines.visualization.matplotlib.decomposition import (
    DecompositionDashboard,
    DecompositionPlot,
    MSTLDecompositionPlot,
    MultiSensorDecompositionPlot,
)
from src.sdk.python.rtdip_sdk.pipelines.visualization.validation import (
    VisualizationDataError,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


@pytest.fixture
def stl_decomposition_data():
    """Create sample STL/Classical decomposition data."""
    np.random.seed(42)
    n = 365
    timestamps = pd.date_range("2024-01-01", periods=n, freq="D")
    trend = np.linspace(10, 20, n)
    seasonal = 5 * np.sin(2 * np.pi * np.arange(n) / 7)
    residual = np.random.randn(n) * 0.5
    value = trend + seasonal + residual

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "value": value,
            "trend": trend,
            "seasonal": seasonal,
            "residual": residual,
        }
    )


@pytest.fixture
def mstl_decomposition_data():
    """Create sample MSTL decomposition data with multiple seasonal components."""
    np.random.seed(42)
    n = 24 * 60  # 60 days hourly
    timestamps = pd.date_range("2024-01-01", periods=n, freq="h")
    trend = np.linspace(10, 15, n)
    seasonal_24 = 5 * np.sin(2 * np.pi * np.arange(n) / 24)
    seasonal_168 = 3 * np.sin(2 * np.pi * np.arange(n) / 168)
    residual = np.random.randn(n) * 0.5
    value = trend + seasonal_24 + seasonal_168 + residual

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "value": value,
            "trend": trend,
            "seasonal_24": seasonal_24,
            "seasonal_168": seasonal_168,
            "residual": residual,
        }
    )


@pytest.fixture
def multi_sensor_decomposition_data(stl_decomposition_data):
    """Create sample multi-sensor decomposition data."""
    data = {}
    for sensor_id in ["SENSOR_001", "SENSOR_002", "SENSOR_003"]:
        df = stl_decomposition_data.copy()
        df["value"] = df["value"] + np.random.randn(len(df)) * 0.1
        data[sensor_id] = df
    return data


class TestDecompositionPlot:
    """Tests for DecompositionPlot class."""

    def test_init(self, stl_decomposition_data):
        """Test DecompositionPlot initialization."""
        plot = DecompositionPlot(
            decomposition_data=stl_decomposition_data,
            sensor_id="SENSOR_001",
        )

        assert plot.decomposition_data is not None
        assert plot.sensor_id == "SENSOR_001"
        assert len(plot._seasonal_columns) == 1
        assert "seasonal" in plot._seasonal_columns

    def test_init_with_mstl_data(self, mstl_decomposition_data):
        """Test DecompositionPlot with MSTL data (multiple seasonals)."""
        plot = DecompositionPlot(
            decomposition_data=mstl_decomposition_data,
            sensor_id="SENSOR_001",
        )

        assert len(plot._seasonal_columns) == 2
        assert "seasonal_24" in plot._seasonal_columns
        assert "seasonal_168" in plot._seasonal_columns

    def test_system_type(self):
        """Test that system_type returns SystemType.PYTHON."""
        assert DecompositionPlot.system_type() == SystemType.PYTHON

    def test_libraries(self):
        """Test that libraries returns a Libraries instance."""
        libraries = DecompositionPlot.libraries()
        assert isinstance(libraries, Libraries)

    def test_settings(self):
        """Test that settings returns an empty dict."""
        settings = DecompositionPlot.settings()
        assert isinstance(settings, dict)
        assert settings == {}

    def test_plot_returns_figure(self, stl_decomposition_data):
        """Test that plot() returns a matplotlib Figure."""
        plot = DecompositionPlot(
            decomposition_data=stl_decomposition_data,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_plot_with_custom_title(self, stl_decomposition_data):
        """Test plot with custom title."""
        plot = DecompositionPlot(
            decomposition_data=stl_decomposition_data,
            title="Custom Decomposition Title",
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_plot_with_column_mapping(self, stl_decomposition_data):
        """Test plot with column mapping."""
        df = stl_decomposition_data.rename(
            columns={"timestamp": "time", "value": "reading"}
        )

        plot = DecompositionPlot(
            decomposition_data=df,
            column_mapping={"time": "timestamp", "reading": "value"},
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_save(self, stl_decomposition_data):
        """Test saving plot to file."""
        plot = DecompositionPlot(
            decomposition_data=stl_decomposition_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_decomposition.png"
            result_path = plot.save(filepath)
            assert result_path.exists()

    def test_invalid_data_raises_error(self):
        """Test that invalid data raises VisualizationDataError."""
        invalid_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        with pytest.raises(VisualizationDataError):
            DecompositionPlot(decomposition_data=invalid_df)

    def test_missing_seasonal_raises_error(self):
        """Test that missing seasonal column raises error."""
        df = pd.DataFrame(
            {
                "timestamp": pd.date_range("2024-01-01", periods=10, freq="D"),
                "value": [1] * 10,
                "trend": [1] * 10,
                "residual": [0] * 10,
            }
        )

        with pytest.raises(VisualizationDataError):
            DecompositionPlot(decomposition_data=df)


class TestMSTLDecompositionPlot:
    """Tests for MSTLDecompositionPlot class."""

    def test_init(self, mstl_decomposition_data):
        """Test MSTLDecompositionPlot initialization."""
        plot = MSTLDecompositionPlot(
            decomposition_data=mstl_decomposition_data,
            sensor_id="SENSOR_001",
        )

        assert plot.decomposition_data is not None
        assert len(plot._seasonal_columns) == 2

    def test_detects_multiple_seasonals(self, mstl_decomposition_data):
        """Test that multiple seasonal columns are detected."""
        plot = MSTLDecompositionPlot(
            decomposition_data=mstl_decomposition_data,
        )

        assert "seasonal_24" in plot._seasonal_columns
        assert "seasonal_168" in plot._seasonal_columns

    def test_plot_returns_figure(self, mstl_decomposition_data):
        """Test that plot() returns a matplotlib Figure."""
        plot = MSTLDecompositionPlot(
            decomposition_data=mstl_decomposition_data,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_zoom_periods(self, mstl_decomposition_data):
        """Test plot with zoomed seasonal panels."""
        plot = MSTLDecompositionPlot(
            decomposition_data=mstl_decomposition_data,
            zoom_periods={"seasonal_24": 168},  # Show 1 week
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_save(self, mstl_decomposition_data):
        """Test saving plot to file."""
        plot = MSTLDecompositionPlot(
            decomposition_data=mstl_decomposition_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_mstl_decomposition.png"
            result_path = plot.save(filepath)
            assert result_path.exists()


class TestDecompositionDashboard:
    """Tests for DecompositionDashboard class."""

    def test_init(self, stl_decomposition_data):
        """Test DecompositionDashboard initialization."""
        dashboard = DecompositionDashboard(
            decomposition_data=stl_decomposition_data,
            sensor_id="SENSOR_001",
        )

        assert dashboard.decomposition_data is not None
        assert dashboard.show_statistics is True

    def test_statistics_calculation(self, stl_decomposition_data):
        """Test statistics calculation."""
        dashboard = DecompositionDashboard(
            decomposition_data=stl_decomposition_data,
        )

        stats = dashboard.get_statistics()

        assert "variance_explained" in stats
        assert "seasonality_strength" in stats
        assert "residual_diagnostics" in stats

        assert "trend" in stats["variance_explained"]
        assert "residual" in stats["variance_explained"]

        diag = stats["residual_diagnostics"]
        assert "mean" in diag
        assert "std" in diag
        assert "skewness" in diag
        assert "kurtosis" in diag

    def test_variance_percentages_positive(self, stl_decomposition_data):
        """Test that variance percentages are positive."""
        dashboard = DecompositionDashboard(
            decomposition_data=stl_decomposition_data,
        )

        stats = dashboard.get_statistics()

        for component, pct in stats["variance_explained"].items():
            assert pct >= 0, f"{component} variance should be >= 0"

    def test_seasonality_strength_range(self, mstl_decomposition_data):
        """Test that seasonality strength is in [0, 1] range."""
        dashboard = DecompositionDashboard(
            decomposition_data=mstl_decomposition_data,
        )

        stats = dashboard.get_statistics()

        for col, strength in stats["seasonality_strength"].items():
            assert 0 <= strength <= 1, f"{col} strength should be in [0, 1]"

    def test_plot_returns_figure(self, stl_decomposition_data):
        """Test that plot() returns a matplotlib Figure."""
        dashboard = DecompositionDashboard(
            decomposition_data=stl_decomposition_data,
        )

        fig = dashboard.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_plot_without_statistics(self, stl_decomposition_data):
        """Test plot without statistics panel."""
        dashboard = DecompositionDashboard(
            decomposition_data=stl_decomposition_data,
            show_statistics=False,
        )

        fig = dashboard.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_save(self, stl_decomposition_data):
        """Test saving dashboard to file."""
        dashboard = DecompositionDashboard(
            decomposition_data=stl_decomposition_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_dashboard.png"
            result_path = dashboard.save(filepath)
            assert result_path.exists()


class TestMultiSensorDecompositionPlot:
    """Tests for MultiSensorDecompositionPlot class."""

    def test_init(self, multi_sensor_decomposition_data):
        """Test MultiSensorDecompositionPlot initialization."""
        plot = MultiSensorDecompositionPlot(
            decomposition_dict=multi_sensor_decomposition_data,
        )

        assert len(plot.decomposition_dict) == 3

    def test_empty_dict_raises_error(self):
        """Test that empty dict raises VisualizationDataError."""
        with pytest.raises(VisualizationDataError):
            MultiSensorDecompositionPlot(decomposition_dict={})

    def test_grid_layout(self, multi_sensor_decomposition_data):
        """Test grid layout for multiple sensors."""
        plot = MultiSensorDecompositionPlot(
            decomposition_dict=multi_sensor_decomposition_data,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_max_sensors_limit(self, stl_decomposition_data):
        """Test max_sensors parameter limits displayed sensors."""
        data = {}
        for i in range(10):
            data[f"SENSOR_{i:03d}"] = stl_decomposition_data.copy()

        plot = MultiSensorDecompositionPlot(
            decomposition_dict=data,
            max_sensors=4,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_compact_mode(self, multi_sensor_decomposition_data):
        """Test compact overlay mode."""
        plot = MultiSensorDecompositionPlot(
            decomposition_dict=multi_sensor_decomposition_data,
            compact=True,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_save(self, multi_sensor_decomposition_data):
        """Test saving plot to file."""
        plot = MultiSensorDecompositionPlot(
            decomposition_dict=multi_sensor_decomposition_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_multi_sensor.png"
            result_path = plot.save(filepath)
            assert result_path.exists()
