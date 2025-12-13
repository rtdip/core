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

"""
Matplotlib-based model comparison visualization components.

This module provides class-based visualization components for comparing
multiple forecasting models, including performance metrics, leaderboards,
and side-by-side forecast comparisons.

Example
--------
```python
from rtdip_sdk.pipelines.visualization.matplotlib.comparison import ModelComparisonPlot

metrics_dict = {
    'AutoGluon': {'mae': 1.23, 'rmse': 2.45, 'mape': 10.5},
    'LSTM': {'mae': 1.45, 'rmse': 2.67, 'mape': 12.3},
    'XGBoost': {'mae': 1.34, 'rmse': 2.56, 'mape': 11.2}
}

plot = ModelComparisonPlot(metrics_dict=metrics_dict)
fig = plot.plot()
plot.save('model_comparison.png')
```
"""

import warnings
from pathlib import Path
from typing import Dict, List, Optional, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas import DataFrame as PandasDataFrame

from .. import config
from .. import utils
from ..interfaces import MatplotlibVisualizationInterface

warnings.filterwarnings("ignore")


class ModelComparisonPlot(MatplotlibVisualizationInterface):
    """
    Create bar chart comparing model performance across metrics.

    This component visualizes the performance comparison of multiple
    models using grouped bar charts.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.comparison import ModelComparisonPlot

    metrics_dict = {
        'AutoGluon': {'mae': 1.23, 'rmse': 2.45, 'mape': 10.5},
        'LSTM': {'mae': 1.45, 'rmse': 2.67, 'mape': 12.3},
    }

    plot = ModelComparisonPlot(
        metrics_dict=metrics_dict,
        metrics_to_plot=['mae', 'rmse']
    )
    fig = plot.plot()
    ```

    Parameters:
        metrics_dict (Dict[str, Dict[str, float]]): Dictionary of
            {model_name: {metric_name: value}}.
        metrics_to_plot (List[str], optional): List of metrics to include.
            Defaults to all metrics in config.METRIC_ORDER.
    """

    metrics_dict: Dict[str, Dict[str, float]]
    metrics_to_plot: Optional[List[str]]
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        metrics_dict: Dict[str, Dict[str, float]],
        metrics_to_plot: Optional[List[str]] = None,
    ) -> None:
        self.metrics_dict = metrics_dict
        self.metrics_to_plot = metrics_to_plot
        self._fig = None
        self._ax = None

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the model comparison visualization.

        Args:
            ax: Optional matplotlib axis to plot on.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        if ax is None:
            self._fig, self._ax = utils.create_figure(
                figsize=config.FIGSIZE["comparison"]
            )
        else:
            self._ax = ax
            self._fig = ax.figure

        df = pd.DataFrame(self.metrics_dict).T

        if self.metrics_to_plot is None:
            metrics_to_plot = [m for m in config.METRIC_ORDER if m in df.columns]
        else:
            metrics_to_plot = [m for m in self.metrics_to_plot if m in df.columns]

        df = df[metrics_to_plot]

        x = np.arange(len(df.columns))
        width = 0.8 / len(df.index)

        models = df.index.tolist()

        for i, model in enumerate(models):
            color = config.get_model_color(model)
            offset = (i - len(models) / 2 + 0.5) * width

            self._ax.bar(
                x + offset,
                df.loc[model],
                width,
                label=model,
                color=color,
                alpha=0.8,
                edgecolor="black",
                linewidth=0.5,
            )

        self._ax.set_xlabel(
            "Metric", fontweight="bold", fontsize=config.FONT_SIZES["axis_label"]
        )
        self._ax.set_ylabel(
            "Value (lower is better)",
            fontweight="bold",
            fontsize=config.FONT_SIZES["axis_label"],
        )
        self._ax.set_title(
            "Model Performance Comparison",
            fontweight="bold",
            fontsize=config.FONT_SIZES["title"],
        )
        self._ax.set_xticks(x)
        self._ax.set_xticklabels(
            [config.METRICS.get(m, {"name": m.upper()})["name"] for m in df.columns]
        )
        self._ax.legend(fontsize=config.FONT_SIZES["legend"])
        utils.add_grid(self._ax)

        plt.tight_layout()

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()
        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )


class ModelMetricsTable(MatplotlibVisualizationInterface):
    """
    Create formatted table of model metrics.

    This component creates a visual table showing metrics for
    multiple models with optional highlighting of best values.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.comparison import ModelMetricsTable

    metrics_dict = {
        'AutoGluon': {'mae': 1.23, 'rmse': 2.45},
        'LSTM': {'mae': 1.45, 'rmse': 2.67},
    }

    table = ModelMetricsTable(
        metrics_dict=metrics_dict,
        highlight_best=True
    )
    fig = table.plot()
    ```

    Parameters:
        metrics_dict (Dict[str, Dict[str, float]]): Dictionary of
            {model_name: {metric_name: value}}.
        highlight_best (bool, optional): Whether to highlight best values.
            Defaults to True.
    """

    metrics_dict: Dict[str, Dict[str, float]]
    highlight_best: bool
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        metrics_dict: Dict[str, Dict[str, float]],
        highlight_best: bool = True,
    ) -> None:
        self.metrics_dict = metrics_dict
        self.highlight_best = highlight_best
        self._fig = None
        self._ax = None

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the metrics table visualization.

        Args:
            ax: Optional matplotlib axis to plot on.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        if ax is None:
            self._fig, self._ax = utils.create_figure(figsize=config.FIGSIZE["single"])
        else:
            self._ax = ax
            self._fig = ax.figure

        self._ax.axis("off")

        df = pd.DataFrame(self.metrics_dict).T

        formatted_data = []
        for model in df.index:
            row = [model]
            for metric in df.columns:
                value = df.loc[model, metric]
                fmt = config.METRICS.get(metric.lower(), {"format": ".3f"})["format"]
                row.append(f"{value:{fmt}}")
            formatted_data.append(row)

        col_labels = ["Model"] + [
            config.METRICS.get(m.lower(), {"name": m.upper()})["name"]
            for m in df.columns
        ]

        table = self._ax.table(
            cellText=formatted_data,
            colLabels=col_labels,
            cellLoc="center",
            loc="center",
            bbox=[0, 0, 1, 1],
        )

        table.auto_set_font_size(False)
        table.set_fontsize(config.FONT_SIZES["legend"])
        table.scale(1, 2)

        for i in range(len(col_labels)):
            table[(0, i)].set_facecolor("#2C3E50")
            table[(0, i)].set_text_props(weight="bold", color="white")

        if self.highlight_best:
            for col_idx, metric in enumerate(df.columns, start=1):
                best_idx = df[metric].idxmin()
                row_idx = list(df.index).index(best_idx) + 1
                table[(row_idx, col_idx)].set_facecolor("#d4edda")
                table[(row_idx, col_idx)].set_text_props(weight="bold")

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()
        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )


class ModelLeaderboardPlot(MatplotlibVisualizationInterface):
    """
    Create horizontal bar chart showing model ranking.

    This component visualizes model performance as a leaderboard
    with horizontal bars sorted by score.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.comparison import ModelLeaderboardPlot

    leaderboard_df = pd.DataFrame({
        'model': ['AutoGluon', 'LSTM', 'XGBoost'],
        'score_val': [0.95, 0.88, 0.91]
    })

    plot = ModelLeaderboardPlot(
        leaderboard_df=leaderboard_df,
        score_column='score_val',
        model_column='model',
        top_n=10
    )
    fig = plot.plot()
    ```

    Parameters:
        leaderboard_df (PandasDataFrame): DataFrame with model scores.
        score_column (str, optional): Column name containing scores.
            Defaults to 'score_val'.
        model_column (str, optional): Column name containing model names.
            Defaults to 'model'.
        top_n (int, optional): Number of top models to show. Defaults to 10.
    """

    leaderboard_df: PandasDataFrame
    score_column: str
    model_column: str
    top_n: int
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        leaderboard_df: PandasDataFrame,
        score_column: str = "score_val",
        model_column: str = "model",
        top_n: int = 10,
    ) -> None:
        self.leaderboard_df = leaderboard_df
        self.score_column = score_column
        self.model_column = model_column
        self.top_n = top_n
        self._fig = None
        self._ax = None

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the leaderboard visualization.

        Args:
            ax: Optional matplotlib axis to plot on.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        if ax is None:
            self._fig, self._ax = utils.create_figure(figsize=config.FIGSIZE["single"])
        else:
            self._ax = ax
            self._fig = ax.figure

        top_models = self.leaderboard_df.nlargest(self.top_n, self.score_column)

        bars = self._ax.barh(
            top_models[self.model_column],
            top_models[self.score_column],
            color=config.COLORS["forecast"],
            alpha=0.7,
            edgecolor="black",
            linewidth=0.5,
        )

        if len(bars) > 0:
            bars[0].set_color(config.MODEL_COLORS["autogluon"])
            bars[0].set_alpha(0.9)

        self._ax.set_xlabel(
            "Validation Score (higher is better)",
            fontweight="bold",
            fontsize=config.FONT_SIZES["axis_label"],
        )
        self._ax.set_title(
            "Model Leaderboard",
            fontweight="bold",
            fontsize=config.FONT_SIZES["title"],
        )
        self._ax.invert_yaxis()
        utils.add_grid(self._ax)

        plt.tight_layout()

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()
        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )


class ModelsOverlayPlot(MatplotlibVisualizationInterface):
    """
    Overlay multiple model forecasts on a single plot.

    This component visualizes forecasts from multiple models
    on the same axes for direct comparison.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.comparison import ModelsOverlayPlot

    predictions_dict = {
        'AutoGluon': autogluon_predictions_df,
        'LSTM': lstm_predictions_df,
        'XGBoost': xgboost_predictions_df
    }

    plot = ModelsOverlayPlot(
        predictions_dict=predictions_dict,
        sensor_id='SENSOR_001',
        actual_data=actual_df
    )
    fig = plot.plot()
    ```

    Parameters:
        predictions_dict (Dict[str, PandasDataFrame]): Dictionary of
            {model_name: predictions_df}. Each df must have columns
            ['item_id', 'timestamp', 'mean' or 'prediction'].
        sensor_id (str): Sensor to plot.
        actual_data (PandasDataFrame, optional): Optional actual values to overlay.
    """

    predictions_dict: Dict[str, PandasDataFrame]
    sensor_id: str
    actual_data: Optional[PandasDataFrame]
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        predictions_dict: Dict[str, PandasDataFrame],
        sensor_id: str,
        actual_data: Optional[PandasDataFrame] = None,
    ) -> None:
        self.predictions_dict = predictions_dict
        self.sensor_id = sensor_id
        self.actual_data = actual_data
        self._fig = None
        self._ax = None

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the models overlay visualization.

        Args:
            ax: Optional matplotlib axis to plot on.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        if ax is None:
            self._fig, self._ax = utils.create_figure(figsize=config.FIGSIZE["single"])
        else:
            self._ax = ax
            self._fig = ax.figure

        markers = ["o", "s", "^", "D", "v", "<", ">", "p"]

        for idx, (model_name, pred_df) in enumerate(self.predictions_dict.items()):
            sensor_data = pred_df[pred_df["item_id"] == self.sensor_id].sort_values(
                "timestamp"
            )

            pred_col = "mean" if "mean" in sensor_data.columns else "prediction"
            color = config.get_model_color(model_name)
            marker = markers[idx % len(markers)]

            self._ax.plot(
                sensor_data["timestamp"],
                sensor_data[pred_col],
                marker=marker,
                linestyle="-",
                label=model_name,
                color=color,
                linewidth=config.LINE_SETTINGS["linewidth"],
                markersize=config.LINE_SETTINGS["marker_size"],
                alpha=0.8,
            )

        if self.actual_data is not None:
            actual_sensor = self.actual_data[
                self.actual_data["item_id"] == self.sensor_id
            ].sort_values("timestamp")
            if len(actual_sensor) > 0:
                self._ax.plot(
                    actual_sensor["timestamp"],
                    actual_sensor["value"],
                    "k--",
                    label="Actual",
                    linewidth=2,
                    alpha=0.7,
                )

        utils.format_axis(
            self._ax,
            title=f"Model Comparison - {self.sensor_id}",
            xlabel="Time",
            ylabel="Value",
            add_legend=True,
            grid=True,
            time_axis=True,
        )

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()
        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )


class ForecastDistributionPlot(MatplotlibVisualizationInterface):
    """
    Box plot comparing forecast distributions across models.

    This component visualizes the distribution of predictions
    from multiple models using box plots.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.comparison import ForecastDistributionPlot

    predictions_dict = {
        'AutoGluon': autogluon_predictions_df,
        'LSTM': lstm_predictions_df,
    }

    plot = ForecastDistributionPlot(
        predictions_dict=predictions_dict,
        show_stats=True
    )
    fig = plot.plot()
    ```

    Parameters:
        predictions_dict (Dict[str, PandasDataFrame]): Dictionary of
            {model_name: predictions_df}.
        show_stats (bool, optional): Whether to show mean markers.
            Defaults to True.
    """

    predictions_dict: Dict[str, PandasDataFrame]
    show_stats: bool
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        predictions_dict: Dict[str, PandasDataFrame],
        show_stats: bool = True,
    ) -> None:
        self.predictions_dict = predictions_dict
        self.show_stats = show_stats
        self._fig = None
        self._ax = None

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the forecast distribution visualization.

        Args:
            ax: Optional matplotlib axis to plot on.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        if ax is None:
            self._fig, self._ax = utils.create_figure(
                figsize=config.FIGSIZE["comparison"]
            )
        else:
            self._ax = ax
            self._fig = ax.figure

        data = []
        labels = []
        colors = []

        for model_name, pred_df in self.predictions_dict.items():
            pred_col = "mean" if "mean" in pred_df.columns else "prediction"
            data.append(pred_df[pred_col].values)
            labels.append(model_name)
            colors.append(config.get_model_color(model_name))

        bp = self._ax.boxplot(
            data,
            labels=labels,
            patch_artist=True,
            showmeans=self.show_stats,
            meanprops=dict(marker="D", markerfacecolor="red", markersize=8),
        )

        for patch, color in zip(bp["boxes"], colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.6)
            patch.set_edgecolor("black")
            patch.set_linewidth(1)

        self._ax.set_ylabel(
            "Predicted Value",
            fontweight="bold",
            fontsize=config.FONT_SIZES["axis_label"],
        )
        self._ax.set_title(
            "Forecast Distribution Comparison",
            fontweight="bold",
            fontsize=config.FONT_SIZES["title"],
        )
        utils.add_grid(self._ax)

        plt.tight_layout()

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()
        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )


class ComparisonDashboard(MatplotlibVisualizationInterface):
    """
    Create comprehensive model comparison dashboard.

    This component creates a dashboard including model performance
    comparison, forecast distributions, overlaid forecasts, and
    metrics table.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.comparison import ComparisonDashboard

    dashboard = ComparisonDashboard(
        predictions_dict=predictions_dict,
        metrics_dict=metrics_dict,
        sensor_id='SENSOR_001',
        actual_data=actual_df
    )
    fig = dashboard.plot()
    dashboard.save('comparison_dashboard.png')
    ```

    Parameters:
        predictions_dict (Dict[str, PandasDataFrame]): Dictionary of
            {model_name: predictions_df}.
        metrics_dict (Dict[str, Dict[str, float]]): Dictionary of
            {model_name: {metric: value}}.
        sensor_id (str): Sensor to visualize.
        actual_data (PandasDataFrame, optional): Optional actual values.
    """

    predictions_dict: Dict[str, PandasDataFrame]
    metrics_dict: Dict[str, Dict[str, float]]
    sensor_id: str
    actual_data: Optional[PandasDataFrame]
    _fig: Optional[plt.Figure]

    def __init__(
        self,
        predictions_dict: Dict[str, PandasDataFrame],
        metrics_dict: Dict[str, Dict[str, float]],
        sensor_id: str,
        actual_data: Optional[PandasDataFrame] = None,
    ) -> None:
        self.predictions_dict = predictions_dict
        self.metrics_dict = metrics_dict
        self.sensor_id = sensor_id
        self.actual_data = actual_data
        self._fig = None

    def plot(self) -> plt.Figure:
        """
        Generate the comparison dashboard.

        Returns:
            matplotlib.figure.Figure: The generated dashboard figure.
        """
        utils.setup_plot_style()

        self._fig = plt.figure(figsize=config.FIGSIZE["dashboard"])
        gs = self._fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)

        ax1 = self._fig.add_subplot(gs[0, 0])
        comparison_plot = ModelComparisonPlot(self.metrics_dict)
        comparison_plot.plot(ax=ax1)

        ax2 = self._fig.add_subplot(gs[0, 1])
        dist_plot = ForecastDistributionPlot(self.predictions_dict)
        dist_plot.plot(ax=ax2)

        ax3 = self._fig.add_subplot(gs[1, 0])
        overlay_plot = ModelsOverlayPlot(
            self.predictions_dict, self.sensor_id, self.actual_data
        )
        overlay_plot.plot(ax=ax3)

        ax4 = self._fig.add_subplot(gs[1, 1])
        table_plot = ModelMetricsTable(self.metrics_dict)
        table_plot.plot(ax=ax4)

        self._fig.suptitle(
            "Model Comparison Dashboard",
            fontsize=config.FONT_SIZES["title"] + 2,
            fontweight="bold",
            y=0.98,
        )

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()
        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )
