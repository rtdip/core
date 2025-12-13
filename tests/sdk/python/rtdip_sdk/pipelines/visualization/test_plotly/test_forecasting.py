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

"""Tests for Plotly forecasting visualization components."""

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest

from src.sdk.python.rtdip_sdk.pipelines.visualization.plotly.forecasting import (
    ErrorDistributionPlotInteractive,
    ForecastComparisonPlotInteractive,
    ForecastPlotInteractive,
    ResidualPlotInteractive,
    ScatterPlotInteractive,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


@pytest.fixture
def sample_historical_data():
    """Create sample historical data for testing."""
    np.random.seed(42)
    timestamps = pd.date_range("2024-01-01", periods=100, freq="h")
    values = np.sin(np.arange(100) * 0.1) + np.random.randn(100) * 0.1
    return pd.DataFrame({"timestamp": timestamps, "value": values})


@pytest.fixture
def sample_forecast_data():
    """Create sample forecast data for testing."""
    np.random.seed(42)
    timestamps = pd.date_range("2024-01-05", periods=24, freq="h")
    mean_values = np.sin(np.arange(100, 124) * 0.1) + np.random.randn(24) * 0.05
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "mean": mean_values,
            "0.1": mean_values - 0.5,
            "0.2": mean_values - 0.3,
            "0.8": mean_values + 0.3,
            "0.9": mean_values + 0.5,
        }
    )


@pytest.fixture
def sample_actual_data():
    """Create sample actual data for testing."""
    np.random.seed(42)
    timestamps = pd.date_range("2024-01-05", periods=24, freq="h")
    values = np.sin(np.arange(100, 124) * 0.1) + np.random.randn(24) * 0.1
    return pd.DataFrame({"timestamp": timestamps, "value": values})


@pytest.fixture
def forecast_start():
    """Return forecast start timestamp."""
    return pd.Timestamp("2024-01-05")


class TestForecastPlotInteractive:
    """Tests for ForecastPlotInteractive class."""

    def test_init(self, sample_historical_data, sample_forecast_data, forecast_start):
        """Test ForecastPlotInteractive initialization."""
        plot = ForecastPlotInteractive(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            forecast_start=forecast_start,
            sensor_id="SENSOR_001",
        )

        assert plot.historical_data is not None
        assert plot.forecast_data is not None
        assert plot.sensor_id == "SENSOR_001"
        assert plot.ci_levels == [60, 80]

    def test_system_type(self):
        """Test that system_type returns SystemType.PYTHON."""
        assert ForecastPlotInteractive.system_type() == SystemType.PYTHON

    def test_libraries(self):
        """Test that libraries returns a Libraries instance."""
        libraries = ForecastPlotInteractive.libraries()
        assert isinstance(libraries, Libraries)

    def test_plot_returns_plotly_figure(
        self, sample_historical_data, sample_forecast_data, forecast_start
    ):
        """Test that plot() returns a Plotly Figure."""
        plot = ForecastPlotInteractive(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            forecast_start=forecast_start,
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)

    def test_save_html(
        self, sample_historical_data, sample_forecast_data, forecast_start
    ):
        """Test saving plot to HTML file."""
        plot = ForecastPlotInteractive(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            forecast_start=forecast_start,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_forecast.html"
            saved_path = plot.save(filepath, format="html")
            assert saved_path.exists()
            assert str(saved_path).endswith(".html")


class TestForecastComparisonPlotInteractive:
    """Tests for ForecastComparisonPlotInteractive class."""

    def test_init(
        self,
        sample_historical_data,
        sample_forecast_data,
        sample_actual_data,
        forecast_start,
    ):
        """Test ForecastComparisonPlotInteractive initialization."""
        plot = ForecastComparisonPlotInteractive(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            actual_data=sample_actual_data,
            forecast_start=forecast_start,
            sensor_id="SENSOR_001",
        )

        assert plot.historical_data is not None
        assert plot.actual_data is not None
        assert plot.sensor_id == "SENSOR_001"

    def test_plot_returns_plotly_figure(
        self,
        sample_historical_data,
        sample_forecast_data,
        sample_actual_data,
        forecast_start,
    ):
        """Test that plot() returns a Plotly Figure."""
        plot = ForecastComparisonPlotInteractive(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            actual_data=sample_actual_data,
            forecast_start=forecast_start,
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)


class TestResidualPlotInteractive:
    """Tests for ResidualPlotInteractive class."""

    def test_init(self, sample_actual_data, sample_forecast_data):
        """Test ResidualPlotInteractive initialization."""
        plot = ResidualPlotInteractive(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
            timestamps=sample_actual_data["timestamp"],
            sensor_id="SENSOR_001",
        )

        assert plot.actual is not None
        assert plot.predicted is not None

    def test_plot_returns_plotly_figure(self, sample_actual_data, sample_forecast_data):
        """Test that plot() returns a Plotly Figure."""
        plot = ResidualPlotInteractive(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
            timestamps=sample_actual_data["timestamp"],
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)


class TestErrorDistributionPlotInteractive:
    """Tests for ErrorDistributionPlotInteractive class."""

    def test_init(self, sample_actual_data, sample_forecast_data):
        """Test ErrorDistributionPlotInteractive initialization."""
        plot = ErrorDistributionPlotInteractive(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
            sensor_id="SENSOR_001",
            bins=20,
        )

        assert plot.bins == 20
        assert plot.sensor_id == "SENSOR_001"

    def test_plot_returns_plotly_figure(self, sample_actual_data, sample_forecast_data):
        """Test that plot() returns a Plotly Figure."""
        plot = ErrorDistributionPlotInteractive(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)


class TestScatterPlotInteractive:
    """Tests for ScatterPlotInteractive class."""

    def test_init(self, sample_actual_data, sample_forecast_data):
        """Test ScatterPlotInteractive initialization."""
        plot = ScatterPlotInteractive(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
            sensor_id="SENSOR_001",
        )

        assert plot.sensor_id == "SENSOR_001"

    def test_plot_returns_plotly_figure(self, sample_actual_data, sample_forecast_data):
        """Test that plot() returns a Plotly Figure."""
        plot = ScatterPlotInteractive(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)
