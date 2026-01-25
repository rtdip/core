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
Plotly-based interactive model comparison visualization components.

This module provides class-based interactive visualization components for
comparing multiple forecasting models using Plotly.

Example
--------
```python
from rtdip_sdk.pipelines.visualization.plotly.comparison import ModelComparisonPlotInteractive

metrics_dict = {
    'AutoGluon': {'mae': 1.23, 'rmse': 2.45, 'mape': 10.5},
    'LSTM': {'mae': 1.45, 'rmse': 2.67, 'mape': 12.3},
    'XGBoost': {'mae': 1.34, 'rmse': 2.56, 'mape': 11.2}
}

plot = ModelComparisonPlotInteractive(metrics_dict=metrics_dict)
fig = plot.plot()
plot.save('model_comparison.html')
```
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd
import plotly.graph_objects as go
from pandas import DataFrame as PandasDataFrame

from .. import config
from ..interfaces import PlotlyVisualizationInterface


class ModelComparisonPlotInteractive(PlotlyVisualizationInterface):
    """
    Create interactive bar chart comparing model performance across metrics.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.comparison import ModelComparisonPlotInteractive

    metrics_dict = {
        'AutoGluon': {'mae': 1.23, 'rmse': 2.45, 'mape': 10.5},
        'LSTM': {'mae': 1.45, 'rmse': 2.67, 'mape': 12.3},
    }

    plot = ModelComparisonPlotInteractive(
        metrics_dict=metrics_dict,
        metrics_to_plot=['mae', 'rmse']
    )
    fig = plot.plot()
    ```

    Parameters:
        metrics_dict (Dict[str, Dict[str, float]]): Dictionary of
            {model_name: {metric_name: value}}.
        metrics_to_plot (List[str], optional): List of metrics to include.
    """

    metrics_dict: Dict[str, Dict[str, float]]
    metrics_to_plot: Optional[List[str]]
    _fig: Optional[go.Figure]

    def __init__(
        self,
        metrics_dict: Dict[str, Dict[str, float]],
        metrics_to_plot: Optional[List[str]] = None,
    ) -> None:
        self.metrics_dict = metrics_dict
        self.metrics_to_plot = metrics_to_plot
        self._fig = None

    def plot(self) -> go.Figure:
        """
        Generate the interactive model comparison visualization.

        Returns:
            plotly.graph_objects.Figure: The generated interactive figure.
        """
        self._fig = go.Figure()

        df = pd.DataFrame(self.metrics_dict).T

        if self.metrics_to_plot is None:
            metrics_to_plot = [m for m in config.METRIC_ORDER if m in df.columns]
        else:
            metrics_to_plot = [m for m in self.metrics_to_plot if m in df.columns]

        df = df[metrics_to_plot]

        for model in df.index:
            color = config.get_model_color(model)
            metric_names = [
                config.METRICS.get(m, {"name": m.upper()})["name"] for m in df.columns
            ]

            self._fig.add_trace(
                go.Bar(
                    name=model,
                    x=metric_names,
                    y=df.loc[model].values,
                    marker_color=color,
                    opacity=0.8,
                    hovertemplate=f"<b>{model}</b><br>%{{x}}: %{{y:.3f}}<extra></extra>",
                )
            )

        self._fig.update_layout(
            title="Model Performance Comparison",
            xaxis_title="Metric",
            yaxis_title="Value (lower is better)",
            barmode="group",
            template="plotly_white",
            height=500,
            legend=dict(x=0.01, y=0.99, bgcolor="rgba(255,255,255,0.8)"),
        )

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        format: str = "html",
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()

        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if format == "html":
            if not str(filepath).endswith(".html"):
                filepath = filepath.with_suffix(".html")
            self._fig.write_html(filepath)
        elif format == "png":
            if not str(filepath).endswith(".png"):
                filepath = filepath.with_suffix(".png")
            self._fig.write_image(
                filepath,
                width=kwargs.get("width", 1200),
                height=kwargs.get("height", 800),
                scale=kwargs.get("scale", 2),
            )
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'html' or 'png'.")

        print(f"Saved: {filepath}")
        return filepath


class ModelsOverlayPlotInteractive(PlotlyVisualizationInterface):
    """
    Overlay multiple model forecasts on a single interactive plot.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.comparison import ModelsOverlayPlotInteractive

    predictions_dict = {
        'AutoGluon': autogluon_predictions_df,
        'LSTM': lstm_predictions_df,
    }

    plot = ModelsOverlayPlotInteractive(
        predictions_dict=predictions_dict,
        sensor_id='SENSOR_001',
        actual_data=actual_df
    )
    fig = plot.plot()
    ```

    Parameters:
        predictions_dict (Dict[str, PandasDataFrame]): Dictionary of
            {model_name: predictions_df}.
        sensor_id (str): Sensor to plot.
        actual_data (PandasDataFrame, optional): Optional actual values to overlay.
    """

    predictions_dict: Dict[str, PandasDataFrame]
    sensor_id: str
    actual_data: Optional[PandasDataFrame]
    _fig: Optional[go.Figure]

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

    def plot(self) -> go.Figure:
        """Generate the interactive models overlay visualization."""
        self._fig = go.Figure()

        symbols = ["circle", "square", "diamond", "triangle-up", "triangle-down"]

        for idx, (model_name, pred_df) in enumerate(self.predictions_dict.items()):
            sensor_data = pred_df[pred_df["item_id"] == self.sensor_id].sort_values(
                "timestamp"
            )

            pred_col = "mean" if "mean" in sensor_data.columns else "prediction"
            color = config.get_model_color(model_name)
            symbol = symbols[idx % len(symbols)]

            self._fig.add_trace(
                go.Scatter(
                    x=sensor_data["timestamp"],
                    y=sensor_data[pred_col],
                    mode="lines+markers",
                    name=model_name,
                    line=dict(color=color, width=2),
                    marker=dict(symbol=symbol, size=6),
                    hovertemplate=f"<b>{model_name}</b><br>Time: %{{x}}<br>Value: %{{y:.2f}}<extra></extra>",
                )
            )

        if self.actual_data is not None:
            actual_sensor = self.actual_data[
                self.actual_data["item_id"] == self.sensor_id
            ].sort_values("timestamp")
            if len(actual_sensor) > 0:
                self._fig.add_trace(
                    go.Scatter(
                        x=actual_sensor["timestamp"],
                        y=actual_sensor["value"],
                        mode="lines",
                        name="Actual",
                        line=dict(color="black", width=2, dash="dash"),
                        hovertemplate="<b>Actual</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>",
                    )
                )

        self._fig.update_layout(
            title=f"Model Comparison - {self.sensor_id}",
            xaxis_title="Time",
            yaxis_title="Value",
            hovermode="x unified",
            template="plotly_white",
            height=600,
            legend=dict(x=0.01, y=0.99, bgcolor="rgba(255,255,255,0.8)"),
        )

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        format: str = "html",
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()

        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if format == "html":
            if not str(filepath).endswith(".html"):
                filepath = filepath.with_suffix(".html")
            self._fig.write_html(filepath)
        elif format == "png":
            if not str(filepath).endswith(".png"):
                filepath = filepath.with_suffix(".png")
            self._fig.write_image(
                filepath,
                width=kwargs.get("width", 1200),
                height=kwargs.get("height", 800),
                scale=kwargs.get("scale", 2),
            )
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'html' or 'png'.")

        print(f"Saved: {filepath}")
        return filepath


class ForecastDistributionPlotInteractive(PlotlyVisualizationInterface):
    """
    Interactive box plot comparing forecast distributions across models.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.comparison import ForecastDistributionPlotInteractive

    predictions_dict = {
        'AutoGluon': autogluon_predictions_df,
        'LSTM': lstm_predictions_df,
    }

    plot = ForecastDistributionPlotInteractive(
        predictions_dict=predictions_dict
    )
    fig = plot.plot()
    ```

    Parameters:
        predictions_dict (Dict[str, PandasDataFrame]): Dictionary of
            {model_name: predictions_df}.
    """

    predictions_dict: Dict[str, PandasDataFrame]
    _fig: Optional[go.Figure]

    def __init__(
        self,
        predictions_dict: Dict[str, PandasDataFrame],
    ) -> None:
        self.predictions_dict = predictions_dict
        self._fig = None

    def plot(self) -> go.Figure:
        """Generate the interactive forecast distribution visualization."""
        self._fig = go.Figure()

        for model_name, pred_df in self.predictions_dict.items():
            pred_col = "mean" if "mean" in pred_df.columns else "prediction"
            color = config.get_model_color(model_name)

            self._fig.add_trace(
                go.Box(
                    y=pred_df[pred_col],
                    name=model_name,
                    marker_color=color,
                    boxmean=True,
                    hovertemplate=f"<b>{model_name}</b><br>Value: %{{y:.2f}}<extra></extra>",
                )
            )

        self._fig.update_layout(
            title="Forecast Distribution Comparison",
            yaxis_title="Predicted Value",
            template="plotly_white",
            height=500,
            showlegend=False,
        )

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        format: str = "html",
        **kwargs,
    ) -> Path:
        """Save the visualization to file."""
        if self._fig is None:
            self.plot()

        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if format == "html":
            if not str(filepath).endswith(".html"):
                filepath = filepath.with_suffix(".html")
            self._fig.write_html(filepath)
        elif format == "png":
            if not str(filepath).endswith(".png"):
                filepath = filepath.with_suffix(".png")
            self._fig.write_image(
                filepath,
                width=kwargs.get("width", 1200),
                height=kwargs.get("height", 800),
                scale=kwargs.get("scale", 2),
            )
        else:
            raise ValueError(f"Unsupported format: {format}. Use 'html' or 'png'.")

        print(f"Saved: {filepath}")
        return filepath
