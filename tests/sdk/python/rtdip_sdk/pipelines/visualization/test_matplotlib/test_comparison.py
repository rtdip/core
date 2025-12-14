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

"""Tests for matplotlib comparison visualization components."""

import tempfile
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytest

from src.sdk.python.rtdip_sdk.pipelines.visualization.matplotlib.comparison import (
    ComparisonDashboard,
    ForecastDistributionPlot,
    ModelComparisonPlot,
    ModelLeaderboardPlot,
    ModelMetricsTable,
    ModelsOverlayPlot,
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


@pytest.fixture
def sample_leaderboard_df():
    """Create sample leaderboard dataframe for testing."""
    return pd.DataFrame(
        {
            "model": ["AutoGluon", "XGBoost", "LSTM", "Prophet", "ARIMA"],
            "score_val": [0.95, 0.91, 0.88, 0.85, 0.82],
        }
    )


class TestModelComparisonPlot:
    """Tests for ModelComparisonPlot class."""

    def test_init(self, sample_metrics_dict):
        """Test ModelComparisonPlot initialization."""
        plot = ModelComparisonPlot(
            metrics_dict=sample_metrics_dict,
            metrics_to_plot=["mae", "rmse"],
        )

        assert plot.metrics_dict is not None
        assert plot.metrics_to_plot == ["mae", "rmse"]

    def test_system_type(self):
        """Test that system_type returns SystemType.PYTHON."""
        assert ModelComparisonPlot.system_type() == SystemType.PYTHON

    def test_libraries(self):
        """Test that libraries returns a Libraries instance."""
        libraries = ModelComparisonPlot.libraries()
        assert isinstance(libraries, Libraries)

    def test_plot_returns_figure(self, sample_metrics_dict):
        """Test that plot() returns a matplotlib Figure."""
        plot = ModelComparisonPlot(metrics_dict=sample_metrics_dict)

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_save(self, sample_metrics_dict):
        """Test saving plot to file."""
        plot = ModelComparisonPlot(metrics_dict=sample_metrics_dict)

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_comparison.png"
            saved_path = plot.save(filepath, verbose=False)
            assert saved_path.exists()


class TestModelMetricsTable:
    """Tests for ModelMetricsTable class."""

    def test_init(self, sample_metrics_dict):
        """Test ModelMetricsTable initialization."""
        table = ModelMetricsTable(
            metrics_dict=sample_metrics_dict,
            highlight_best=True,
        )

        assert table.metrics_dict is not None
        assert table.highlight_best is True

    def test_plot_returns_figure(self, sample_metrics_dict):
        """Test that plot() returns a matplotlib Figure."""
        table = ModelMetricsTable(metrics_dict=sample_metrics_dict)

        fig = table.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestModelLeaderboardPlot:
    """Tests for ModelLeaderboardPlot class."""

    def test_init(self, sample_leaderboard_df):
        """Test ModelLeaderboardPlot initialization."""
        plot = ModelLeaderboardPlot(
            leaderboard_df=sample_leaderboard_df,
            score_column="score_val",
            model_column="model",
            top_n=3,
        )

        assert plot.top_n == 3
        assert plot.score_column == "score_val"

    def test_plot_returns_figure(self, sample_leaderboard_df):
        """Test that plot() returns a matplotlib Figure."""
        plot = ModelLeaderboardPlot(leaderboard_df=sample_leaderboard_df)

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestModelsOverlayPlot:
    """Tests for ModelsOverlayPlot class."""

    def test_init(self, sample_predictions_dict):
        """Test ModelsOverlayPlot initialization."""
        plot = ModelsOverlayPlot(
            predictions_dict=sample_predictions_dict,
            sensor_id="SENSOR_001",
        )

        assert plot.sensor_id == "SENSOR_001"
        assert len(plot.predictions_dict) == 3

    def test_plot_returns_figure(self, sample_predictions_dict):
        """Test that plot() returns a matplotlib Figure."""
        plot = ModelsOverlayPlot(
            predictions_dict=sample_predictions_dict,
            sensor_id="SENSOR_001",
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

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

        plot = ModelsOverlayPlot(
            predictions_dict=sample_predictions_dict,
            sensor_id="SENSOR_001",
            actual_data=actual_data,
        )

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestForecastDistributionPlot:
    """Tests for ForecastDistributionPlot class."""

    def test_init(self, sample_predictions_dict):
        """Test ForecastDistributionPlot initialization."""
        plot = ForecastDistributionPlot(
            predictions_dict=sample_predictions_dict,
            show_stats=True,
        )

        assert plot.show_stats is True
        assert len(plot.predictions_dict) == 3

    def test_plot_returns_figure(self, sample_predictions_dict):
        """Test that plot() returns a matplotlib Figure."""
        plot = ForecastDistributionPlot(predictions_dict=sample_predictions_dict)

        fig = plot.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)


class TestComparisonDashboard:
    """Tests for ComparisonDashboard class."""

    def test_init(self, sample_predictions_dict, sample_metrics_dict):
        """Test ComparisonDashboard initialization."""
        dashboard = ComparisonDashboard(
            predictions_dict=sample_predictions_dict,
            metrics_dict=sample_metrics_dict,
            sensor_id="SENSOR_001",
        )

        assert dashboard.sensor_id == "SENSOR_001"

    def test_plot_returns_figure(self, sample_predictions_dict, sample_metrics_dict):
        """Test that plot() returns a matplotlib Figure."""
        dashboard = ComparisonDashboard(
            predictions_dict=sample_predictions_dict,
            metrics_dict=sample_metrics_dict,
            sensor_id="SENSOR_001",
        )

        fig = dashboard.plot()
        assert isinstance(fig, plt.Figure)
        plt.close(fig)

    def test_save(self, sample_predictions_dict, sample_metrics_dict):
        """Test saving dashboard to file."""
        dashboard = ComparisonDashboard(
            predictions_dict=sample_predictions_dict,
            metrics_dict=sample_metrics_dict,
            sensor_id="SENSOR_001",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "test_dashboard.png"
            saved_path = dashboard.save(filepath, verbose=False)
            assert saved_path.exists()
