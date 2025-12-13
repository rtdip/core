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

"""Tests for matplotlib forecasting visualization components."""

import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from src.sdk.python.rtdip_sdk.pipelines.visualization.matplotlib.forecasting import (
    ErrorDistributionPlot,
    ForecastComparisonPlot,
    ForecastDashboard,
    ForecastPlot,
    MultiSensorForecastPlot,
    ResidualPlot,
    ScatterPlot,
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


class TestForecastPlot:
    """Tests for ForecastPlot class."""

    def test_init(self, sample_historical_data, sample_forecast_data, forecast_start):
        """Test ForecastPlot initialization."""
        plot = ForecastPlot(
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
        assert ForecastPlot.system_type() == SystemType.PYTHON

    def test_libraries(self):
        """Test that libraries returns a Libraries instance."""
        libraries = ForecastPlot.libraries()
        assert isinstance(libraries, Libraries)

    def test_settings(self):
        """Test that settings returns an empty dict."""
        settings = ForecastPlot.settings()
        assert isinstance(settings, dict)
        assert settings == {}

    def test_plot_returns_figure(
        self, sample_historical_data, sample_forecast_data, forecast_start
    ):
        """Test that plot() returns a matplotlib Figure."""
        plot = ForecastPlot(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            forecast_start=forecast_start,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_plot_with_custom_title(
        self, sample_historical_data, sample_forecast_data, forecast_start
    ):
        """Test plot with custom title."""
        plot = ForecastPlot(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            forecast_start=forecast_start,
            title="Custom Title",
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_save(
        self, sample_historical_data, sample_forecast_data, forecast_start
    ):
        """Test saving plot to file."""
        plot = ForecastPlot(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            forecast_start=forecast_start,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_forecast.png"
            saved_path = plot.save(filepath, verbose=False)
            assert saved_path.exists()


class TestForecastComparisonPlot:
    """Tests for ForecastComparisonPlot class."""

    def test_init(
        self,
        sample_historical_data,
        sample_forecast_data,
        sample_actual_data,
        forecast_start,
    ):
        """Test ForecastComparisonPlot initialization."""
        plot = ForecastComparisonPlot(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            actual_data=sample_actual_data,
            forecast_start=forecast_start,
            sensor_id="SENSOR_001",
        )

        assert plot.historical_data is not None
        assert plot.actual_data is not None
        assert plot.sensor_id == "SENSOR_001"

    def test_plot_returns_figure(
        self,
        sample_historical_data,
        sample_forecast_data,
        sample_actual_data,
        forecast_start,
    ):
        """Test that plot() returns a matplotlib Figure."""
        plot = ForecastComparisonPlot(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            actual_data=sample_actual_data,
            forecast_start=forecast_start,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestResidualPlot:
    """Tests for ResidualPlot class."""

    def test_init(self, sample_actual_data, sample_forecast_data):
        """Test ResidualPlot initialization."""
        plot = ResidualPlot(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
            timestamps=sample_actual_data["timestamp"],
            sensor_id="SENSOR_001",
        )

        assert plot.actual is not None
        assert plot.predicted is not None

    def test_plot_returns_figure(self, sample_actual_data, sample_forecast_data):
        """Test that plot() returns a matplotlib Figure."""
        plot = ResidualPlot(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
            timestamps=sample_actual_data["timestamp"],
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestErrorDistributionPlot:
    """Tests for ErrorDistributionPlot class."""

    def test_init(self, sample_actual_data, sample_forecast_data):
        """Test ErrorDistributionPlot initialization."""
        plot = ErrorDistributionPlot(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
            sensor_id="SENSOR_001",
            bins=20,
        )

        assert plot.bins == 20
        assert plot.sensor_id == "SENSOR_001"

    def test_plot_returns_figure(self, sample_actual_data, sample_forecast_data):
        """Test that plot() returns a matplotlib Figure."""
        plot = ErrorDistributionPlot(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestScatterPlot:
    """Tests for ScatterPlot class."""

    def test_init(self, sample_actual_data, sample_forecast_data):
        """Test ScatterPlot initialization."""
        plot = ScatterPlot(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
            sensor_id="SENSOR_001",
            show_metrics=True,
        )

        assert plot.show_metrics is True
        assert plot.sensor_id == "SENSOR_001"

    def test_plot_returns_figure(self, sample_actual_data, sample_forecast_data):
        """Test that plot() returns a matplotlib Figure."""
        plot = ScatterPlot(
            actual=sample_actual_data["value"],
            predicted=sample_forecast_data["mean"],
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestForecastDashboard:
    """Tests for ForecastDashboard class."""

    def test_init(
        self,
        sample_historical_data,
        sample_forecast_data,
        sample_actual_data,
        forecast_start,
    ):
        """Test ForecastDashboard initialization."""
        dashboard = ForecastDashboard(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            actual_data=sample_actual_data,
            forecast_start=forecast_start,
            sensor_id="SENSOR_001",
        )

        assert dashboard.sensor_id == "SENSOR_001"

    def test_plot_returns_figure(
        self,
        sample_historical_data,
        sample_forecast_data,
        sample_actual_data,
        forecast_start,
    ):
        """Test that plot() returns a matplotlib Figure."""
        dashboard = ForecastDashboard(
            historical_data=sample_historical_data,
            forecast_data=sample_forecast_data,
            actual_data=sample_actual_data,
            forecast_start=forecast_start,
        )

        fig = dashboard.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestMultiSensorForecastPlot:
    """Tests for MultiSensorForecastPlot class."""

    @pytest.fixture
    def multi_sensor_predictions(self):
        """Create multi-sensor predictions data."""
        np.random.seed(42)
        data = []
        for sensor in ["SENSOR_001", "SENSOR_002", "SENSOR_003"]:
            timestamps = pd.date_range("2024-01-05", periods=24, freq="h")
            mean_values = np.random.randn(24)
            for ts, mean in zip(timestamps, mean_values):
                data.append(
                    {
                        "item_id": sensor,
                        "timestamp": ts,
                        "mean": mean,
                        "0.1": mean - 0.5,
                        "0.9": mean + 0.5,
                    }
                )
        return pd.DataFrame(data)

    @pytest.fixture
    def multi_sensor_historical(self):
        """Create multi-sensor historical data."""
        np.random.seed(42)
        data = []
        for sensor in ["SENSOR_001", "SENSOR_002", "SENSOR_003"]:
            timestamps = pd.date_range("2024-01-01", periods=100, freq="h")
            values = np.random.randn(100)
            for ts, val in zip(timestamps, values):
                data.append({"TagName": sensor, "EventTime": ts, "Value": val})
        return pd.DataFrame(data)

    def test_init(self, multi_sensor_predictions, multi_sensor_historical):
        """Test MultiSensorForecastPlot initialization."""
        plot = MultiSensorForecastPlot(
            predictions_df=multi_sensor_predictions,
            historical_df=multi_sensor_historical,
            lookback_hours=168,
            max_sensors=3,
        )

        assert plot.max_sensors == 3
        assert plot.lookback_hours == 168

    def test_plot_returns_figure(
        self, multi_sensor_predictions, multi_sensor_historical
    ):
        """Test that plot() returns a matplotlib Figure."""
        plot = MultiSensorForecastPlot(
            predictions_df=multi_sensor_predictions,
            historical_df=multi_sensor_historical,
            max_sensors=3,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)
