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

"""Tests for plotly decomposition visualization components."""

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest

from src.sdk.python.rtdip_sdk.pipelines.visualization.plotly.decomposition import (
    DecompositionDashboardInteractive,
    DecompositionPlotInteractive,
    MSTLDecompositionPlotInteractive,
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


class TestDecompositionPlotInteractive:
    """Tests for DecompositionPlotInteractive class."""

    def test_init(self, stl_decomposition_data):
        """Test DecompositionPlotInteractive initialization."""
        plot = DecompositionPlotInteractive(
            decomposition_data=stl_decomposition_data,
            sensor_id="SENSOR_001",
        )

        assert plot.decomposition_data is not None
        assert plot.sensor_id == "SENSOR_001"
        assert len(plot._seasonal_columns) == 1

    def test_init_with_mstl_data(self, mstl_decomposition_data):
        """Test DecompositionPlotInteractive with MSTL data."""
        plot = DecompositionPlotInteractive(
            decomposition_data=mstl_decomposition_data,
            sensor_id="SENSOR_001",
        )

        assert len(plot._seasonal_columns) == 2
        assert "seasonal_24" in plot._seasonal_columns
        assert "seasonal_168" in plot._seasonal_columns

    def test_system_type(self):
        """Test that system_type returns SystemType.PYTHON."""
        assert DecompositionPlotInteractive.system_type() == SystemType.PYTHON

    def test_libraries(self):
        """Test that libraries returns a Libraries instance."""
        libraries = DecompositionPlotInteractive.libraries()
        assert isinstance(libraries, Libraries)

    def test_plot_returns_figure(self, stl_decomposition_data):
        """Test that plot() returns a Plotly Figure."""
        plot = DecompositionPlotInteractive(
            decomposition_data=stl_decomposition_data,
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)

    def test_plot_with_custom_title(self, stl_decomposition_data):
        """Test plot with custom title."""
        plot = DecompositionPlotInteractive(
            decomposition_data=stl_decomposition_data,
            title="Custom Interactive Title",
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)

    def test_plot_without_rangeslider(self, stl_decomposition_data):
        """Test plot without range slider."""
        plot = DecompositionPlotInteractive(
            decomposition_data=stl_decomposition_data,
            show_rangeslider=False,
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)

    def test_save_html(self, stl_decomposition_data):
        """Test saving plot as HTML."""
        plot = DecompositionPlotInteractive(
            decomposition_data=stl_decomposition_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_decomposition.html"
            result_path = plot.save(filepath, format="html")
            assert result_path.exists()
            assert result_path.suffix == ".html"

    def test_invalid_data_raises_error(self):
        """Test that invalid data raises VisualizationDataError."""
        invalid_df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})

        with pytest.raises(VisualizationDataError):
            DecompositionPlotInteractive(decomposition_data=invalid_df)


class TestMSTLDecompositionPlotInteractive:
    """Tests for MSTLDecompositionPlotInteractive class."""

    def test_init(self, mstl_decomposition_data):
        """Test MSTLDecompositionPlotInteractive initialization."""
        plot = MSTLDecompositionPlotInteractive(
            decomposition_data=mstl_decomposition_data,
            sensor_id="SENSOR_001",
        )

        assert plot.decomposition_data is not None
        assert len(plot._seasonal_columns) == 2

    def test_detects_multiple_seasonals(self, mstl_decomposition_data):
        """Test that multiple seasonal columns are detected."""
        plot = MSTLDecompositionPlotInteractive(
            decomposition_data=mstl_decomposition_data,
        )

        assert "seasonal_24" in plot._seasonal_columns
        assert "seasonal_168" in plot._seasonal_columns

    def test_plot_returns_figure(self, mstl_decomposition_data):
        """Test that plot() returns a Plotly Figure."""
        plot = MSTLDecompositionPlotInteractive(
            decomposition_data=mstl_decomposition_data,
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)

    def test_save_html(self, mstl_decomposition_data):
        """Test saving plot as HTML."""
        plot = MSTLDecompositionPlotInteractive(
            decomposition_data=mstl_decomposition_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_mstl_decomposition.html"
            result_path = plot.save(filepath, format="html")
            assert result_path.exists()


class TestDecompositionDashboardInteractive:
    """Tests for DecompositionDashboardInteractive class."""

    def test_init(self, stl_decomposition_data):
        """Test DecompositionDashboardInteractive initialization."""
        dashboard = DecompositionDashboardInteractive(
            decomposition_data=stl_decomposition_data,
            sensor_id="SENSOR_001",
        )

        assert dashboard.decomposition_data is not None

    def test_statistics_calculation(self, stl_decomposition_data):
        """Test statistics calculation."""
        dashboard = DecompositionDashboardInteractive(
            decomposition_data=stl_decomposition_data,
        )

        stats = dashboard.get_statistics()

        assert "variance_explained" in stats
        assert "seasonality_strength" in stats
        assert "residual_diagnostics" in stats

    def test_variance_percentages_positive(self, stl_decomposition_data):
        """Test that variance percentages are positive."""
        dashboard = DecompositionDashboardInteractive(
            decomposition_data=stl_decomposition_data,
        )

        stats = dashboard.get_statistics()

        for component, pct in stats["variance_explained"].items():
            assert pct >= 0, f"{component} variance should be >= 0"

    def test_seasonality_strength_range(self, mstl_decomposition_data):
        """Test that seasonality strength is in [0, 1] range."""
        dashboard = DecompositionDashboardInteractive(
            decomposition_data=mstl_decomposition_data,
        )

        stats = dashboard.get_statistics()

        for col, strength in stats["seasonality_strength"].items():
            assert 0 <= strength <= 1, f"{col} strength should be in [0, 1]"

    def test_plot_returns_figure(self, stl_decomposition_data):
        """Test that plot() returns a Plotly Figure."""
        dashboard = DecompositionDashboardInteractive(
            decomposition_data=stl_decomposition_data,
        )

        fig = dashboard.plot()
        assert isinstance(fig, go.Figure)

    def test_save_html(self, stl_decomposition_data):
        """Test saving dashboard as HTML."""
        dashboard = DecompositionDashboardInteractive(
            decomposition_data=stl_decomposition_data,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_dashboard.html"
            result_path = dashboard.save(filepath, format="html")
            assert result_path.exists()
