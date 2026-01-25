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

"""Tests for Plotly comparison visualization components."""

import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pytest

from src.sdk.python.rtdip_sdk.pipelines.visualization.plotly.comparison import (
    ForecastDistributionPlotInteractive,
    ModelComparisonPlotInteractive,
    ModelsOverlayPlotInteractive,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


@pytest.fixture
def sample_metrics_dict():
    """Create sample metrics dictionary for testing."""
    return {
        "AutoGluon": {"mae": 1.23, "rmse": 2.45, "mape": 10.5, "r2": 0.85},
        "LSTM": {"mae": 1.45, "rmse": 2.67, "mape": 12.3, "r2": 0.80},
        "XGBoost": {"mae": 1.34, "rmse": 2.56, "mape": 11.2, "r2": 0.82},
    }


@pytest.fixture
def sample_predictions_dict():
    """Create sample predictions dictionary for testing."""
    np.random.seed(42)
    predictions = {}
    for model in ["AutoGluon", "LSTM", "XGBoost"]:
        timestamps = pd.date_range("2024-01-05", periods=24, freq="h")
        predictions[model] = pd.DataFrame(
            {
                "item_id": ["SENSOR_001"] * 24,
                "timestamp": timestamps,
                "mean": np.random.randn(24),
            }
        )
    return predictions


class TestModelComparisonPlotInteractive:
    """Tests for ModelComparisonPlotInteractive class."""

    def test_init(self, sample_metrics_dict):
        """Test ModelComparisonPlotInteractive initialization."""
        plot = ModelComparisonPlotInteractive(
            metrics_dict=sample_metrics_dict,
            metrics_to_plot=["mae", "rmse"],
        )

        assert plot.metrics_dict is not None
        assert plot.metrics_to_plot == ["mae", "rmse"]

    def test_system_type(self):
        """Test that system_type returns SystemType.PYTHON."""
        assert ModelComparisonPlotInteractive.system_type() == SystemType.PYTHON

    def test_libraries(self):
        """Test that libraries returns a Libraries instance."""
        libraries = ModelComparisonPlotInteractive.libraries()
        assert isinstance(libraries, Libraries)

    def test_plot_returns_plotly_figure(self, sample_metrics_dict):
        """Test that plot() returns a Plotly Figure."""
        plot = ModelComparisonPlotInteractive(metrics_dict=sample_metrics_dict)

        fig = plot.plot()
        assert isinstance(fig, go.Figure)

    def test_save_html(self, sample_metrics_dict):
        """Test saving plot to HTML file."""
        plot = ModelComparisonPlotInteractive(metrics_dict=sample_metrics_dict)

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_comparison.html"
            saved_path = plot.save(filepath, format="html")
            assert saved_path.exists()
            assert str(saved_path).endswith(".html")


class TestModelsOverlayPlotInteractive:
    """Tests for ModelsOverlayPlotInteractive class."""

    def test_init(self, sample_predictions_dict):
        """Test ModelsOverlayPlotInteractive initialization."""
        plot = ModelsOverlayPlotInteractive(
            predictions_dict=sample_predictions_dict,
            sensor_id="SENSOR_001",
        )

        assert plot.sensor_id == "SENSOR_001"
        assert len(plot.predictions_dict) == 3

    def test_plot_returns_plotly_figure(self, sample_predictions_dict):
        """Test that plot() returns a Plotly Figure."""
        plot = ModelsOverlayPlotInteractive(
            predictions_dict=sample_predictions_dict,
            sensor_id="SENSOR_001",
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)

    def test_plot_with_actual_data(self, sample_predictions_dict):
        """Test plot with actual data overlay."""
        np.random.seed(42)
        actual_data = pd.DataFrame(
            {
                "item_id": ["SENSOR_001"] * 24,
                "timestamp": pd.date_range("2024-01-05", periods=24, freq="h"),
                "value": np.random.randn(24),
            }
        )

        plot = ModelsOverlayPlotInteractive(
            predictions_dict=sample_predictions_dict,
            sensor_id="SENSOR_001",
            actual_data=actual_data,
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)


class TestForecastDistributionPlotInteractive:
    """Tests for ForecastDistributionPlotInteractive class."""

    def test_init(self, sample_predictions_dict):
        """Test ForecastDistributionPlotInteractive initialization."""
        plot = ForecastDistributionPlotInteractive(
            predictions_dict=sample_predictions_dict,
        )

        assert len(plot.predictions_dict) == 3

    def test_plot_returns_plotly_figure(self, sample_predictions_dict):
        """Test that plot() returns a Plotly Figure."""
        plot = ForecastDistributionPlotInteractive(
            predictions_dict=sample_predictions_dict
        )

        fig = plot.plot()
        assert isinstance(fig, go.Figure)

    def test_save_html(self, sample_predictions_dict):
        """Test saving plot to HTML file."""
        plot = ForecastDistributionPlotInteractive(
            predictions_dict=sample_predictions_dict
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_distribution.html"
            saved_path = plot.save(filepath, format="html")
            assert saved_path.exists()
