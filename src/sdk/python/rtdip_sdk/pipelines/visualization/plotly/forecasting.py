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
Plotly-based interactive forecasting visualization components.

This module provides class-based interactive visualization components for
time series forecasting results using Plotly.

Example
--------
```python
from rtdip_sdk.pipelines.visualization.plotly.forecasting import ForecastPlotInteractive
import pandas as pd

historical_df = pd.DataFrame({
    'timestamp': pd.date_range('2024-01-01', periods=100, freq='h'),
    'value': np.random.randn(100)
})
forecast_df = pd.DataFrame({
    'timestamp': pd.date_range('2024-01-05', periods=24, freq='h'),
    'mean': np.random.randn(24),
    '0.1': np.random.randn(24) - 1,
    '0.9': np.random.randn(24) + 1,
})

plot = ForecastPlotInteractive(
    historical_data=historical_df,
    forecast_data=forecast_df,
    forecast_start=pd.Timestamp('2024-01-05'),
    sensor_id='SENSOR_001'
)
fig = plot.plot()
plot.save('forecast.html')
```
"""

from pathlib import Path
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from pandas import DataFrame as PandasDataFrame

from .. import config
from ..interfaces import PlotlyVisualizationInterface
from ..validation import (
    VisualizationDataError,
    prepare_dataframe,
    check_data_overlap,
)


class ForecastPlotInteractive(PlotlyVisualizationInterface):
    """
    Create interactive Plotly forecast plot with confidence intervals.

    This component creates an interactive visualization showing historical
    data, forecast predictions, and optional confidence interval bands.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.forecasting import ForecastPlotInteractive

    plot = ForecastPlotInteractive(
        historical_data=historical_df,
        forecast_data=forecast_df,
        forecast_start=pd.Timestamp('2024-01-05'),
        sensor_id='SENSOR_001',
        ci_levels=[60, 80]
    )
    fig = plot.plot()
    plot.save('forecast.html')
    ```

    Parameters:
        historical_data (PandasDataFrame): DataFrame with 'timestamp' and 'value' columns.
        forecast_data (PandasDataFrame): DataFrame with 'timestamp', 'mean', and
            quantile columns ('0.1', '0.2', '0.8', '0.9').
        forecast_start (pd.Timestamp): Timestamp marking the start of forecast period.
        sensor_id (str, optional): Sensor identifier for the plot title.
        ci_levels (List[int], optional): Confidence interval levels. Defaults to [60, 80].
        title (str, optional): Custom plot title.
        column_mapping (Dict[str, str], optional): Mapping from your column names to
            expected names. Example: {"time": "timestamp", "reading": "value"}
    """

    historical_data: PandasDataFrame
    forecast_data: PandasDataFrame
    forecast_start: pd.Timestamp
    sensor_id: Optional[str]
    ci_levels: List[int]
    title: Optional[str]
    column_mapping: Optional[Dict[str, str]]
    _fig: Optional[go.Figure]

    def __init__(
        self,
        historical_data: PandasDataFrame,
        forecast_data: PandasDataFrame,
        forecast_start: pd.Timestamp,
        sensor_id: Optional[str] = None,
        ci_levels: Optional[List[int]] = None,
        title: Optional[str] = None,
        column_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        self.column_mapping = column_mapping
        self.sensor_id = sensor_id
        self.ci_levels = ci_levels if ci_levels is not None else [60, 80]
        self.title = title
        self._fig = None

        self.historical_data = prepare_dataframe(
            historical_data,
            required_columns=["timestamp", "value"],
            df_name="historical_data",
            column_mapping=column_mapping,
            datetime_cols=["timestamp"],
            numeric_cols=["value"],
            sort_by="timestamp",
        )

        ci_columns = ["0.05", "0.1", "0.2", "0.8", "0.9", "0.95"]
        self.forecast_data = prepare_dataframe(
            forecast_data,
            required_columns=["timestamp", "mean"],
            df_name="forecast_data",
            column_mapping=column_mapping,
            datetime_cols=["timestamp"],
            numeric_cols=["mean"] + ci_columns,
            optional_columns=ci_columns,
            sort_by="timestamp",
        )

        if forecast_start is None:
            raise VisualizationDataError(
                "forecast_start cannot be None. Please provide a valid timestamp."
            )
        self.forecast_start = pd.to_datetime(forecast_start)

    def plot(self) -> go.Figure:
        """
        Generate the interactive forecast visualization.

        Returns:
            plotly.graph_objects.Figure: The generated interactive figure.
        """
        self._fig = go.Figure()

        self._fig.add_trace(
            go.Scatter(
                x=self.historical_data["timestamp"],
                y=self.historical_data["value"],
                mode="lines",
                name="Historical",
                line=dict(color=config.COLORS["historical"], width=1.5),
                hovertemplate="<b>Historical</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>",
            )
        )

        self._fig.add_trace(
            go.Scatter(
                x=self.forecast_data["timestamp"],
                y=self.forecast_data["mean"],
                mode="lines",
                name="Forecast",
                line=dict(color=config.COLORS["forecast"], width=2),
                hovertemplate="<b>Forecast</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>",
            )
        )

        for ci_level in sorted(self.ci_levels, reverse=True):
            lower_q = (100 - ci_level) / 200
            upper_q = 1 - lower_q

            lower_col = f"{lower_q:.1f}"
            upper_col = f"{upper_q:.1f}"

            if (
                lower_col in self.forecast_data.columns
                and upper_col in self.forecast_data.columns
            ):
                self._fig.add_trace(
                    go.Scatter(
                        x=self.forecast_data["timestamp"],
                        y=self.forecast_data[upper_col],
                        mode="lines",
                        line=dict(width=0),
                        showlegend=False,
                        hoverinfo="skip",
                    )
                )

                self._fig.add_trace(
                    go.Scatter(
                        x=self.forecast_data["timestamp"],
                        y=self.forecast_data[lower_col],
                        mode="lines",
                        fill="tonexty",
                        name=f"{ci_level}% CI",
                        fillcolor=(
                            config.COLORS["ci_60"]
                            if ci_level == 60
                            else config.COLORS["ci_80"]
                        ),
                        opacity=0.3 if ci_level == 60 else 0.2,
                        line=dict(width=0),
                        hovertemplate=f"<b>{ci_level}% CI</b><br>Time: %{{x}}<br>Lower: %{{y:.2f}}<extra></extra>",
                    )
                )

        self._fig.add_shape(
            type="line",
            x0=self.forecast_start,
            x1=self.forecast_start,
            y0=0,
            y1=1,
            yref="paper",
            line=dict(color=config.COLORS["forecast_start"], width=2, dash="dash"),
        )

        self._fig.add_annotation(
            x=self.forecast_start,
            y=1,
            yref="paper",
            text="Forecast Start",
            showarrow=False,
            yshift=10,
        )

        plot_title = self.title or "Forecast with Confidence Intervals"
        if self.sensor_id:
            plot_title += f" - {self.sensor_id}"

        self._fig.update_layout(
            title=plot_title,
            xaxis_title="Time",
            yaxis_title="Value",
            hovermode="x unified",
            template="plotly_white",
            height=600,
            showlegend=True,
            legend=dict(x=0.01, y=0.99, bgcolor="rgba(255,255,255,0.8)"),
        )

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        format: str = "html",
        **kwargs,
    ) -> Path:
        """
        Save the visualization to file.

        Args:
            filepath: Output file path
            format: Output format ('html' or 'png')
            **kwargs: Additional save options (width, height, scale for png)

        Returns:
            Path to the saved file
        """
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


class ForecastComparisonPlotInteractive(PlotlyVisualizationInterface):
    """
    Create interactive Plotly plot comparing forecast against actual values.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.forecasting import ForecastComparisonPlotInteractive

    plot = ForecastComparisonPlotInteractive(
        historical_data=historical_df,
        forecast_data=forecast_df,
        actual_data=actual_df,
        forecast_start=pd.Timestamp('2024-01-05'),
        sensor_id='SENSOR_001'
    )
    fig = plot.plot()
    ```

    Parameters:
        historical_data (PandasDataFrame): DataFrame with 'timestamp' and 'value' columns.
        forecast_data (PandasDataFrame): DataFrame with 'timestamp' and 'mean' columns.
        actual_data (PandasDataFrame): DataFrame with actual values during forecast period.
        forecast_start (pd.Timestamp): Timestamp marking the start of forecast period.
        sensor_id (str, optional): Sensor identifier for the plot title.
        title (str, optional): Custom plot title.
        column_mapping (Dict[str, str], optional): Mapping from your column names to
            expected names.
    """

    historical_data: PandasDataFrame
    forecast_data: PandasDataFrame
    actual_data: PandasDataFrame
    forecast_start: pd.Timestamp
    sensor_id: Optional[str]
    title: Optional[str]
    column_mapping: Optional[Dict[str, str]]
    _fig: Optional[go.Figure]

    def __init__(
        self,
        historical_data: PandasDataFrame,
        forecast_data: PandasDataFrame,
        actual_data: PandasDataFrame,
        forecast_start: pd.Timestamp,
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        column_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        self.column_mapping = column_mapping
        self.sensor_id = sensor_id
        self.title = title
        self._fig = None

        self.historical_data = prepare_dataframe(
            historical_data,
            required_columns=["timestamp", "value"],
            df_name="historical_data",
            column_mapping=column_mapping,
            datetime_cols=["timestamp"],
            numeric_cols=["value"],
            sort_by="timestamp",
        )

        self.forecast_data = prepare_dataframe(
            forecast_data,
            required_columns=["timestamp", "mean"],
            df_name="forecast_data",
            column_mapping=column_mapping,
            datetime_cols=["timestamp"],
            numeric_cols=["mean"],
            sort_by="timestamp",
        )

        self.actual_data = prepare_dataframe(
            actual_data,
            required_columns=["timestamp", "value"],
            df_name="actual_data",
            column_mapping=column_mapping,
            datetime_cols=["timestamp"],
            numeric_cols=["value"],
            sort_by="timestamp",
        )

        if forecast_start is None:
            raise VisualizationDataError(
                "forecast_start cannot be None. Please provide a valid timestamp."
            )
        self.forecast_start = pd.to_datetime(forecast_start)

        check_data_overlap(
            self.forecast_data,
            self.actual_data,
            on="timestamp",
            df1_name="forecast_data",
            df2_name="actual_data",
        )

    def plot(self) -> go.Figure:
        """Generate the interactive forecast comparison visualization."""
        self._fig = go.Figure()

        self._fig.add_trace(
            go.Scatter(
                x=self.historical_data["timestamp"],
                y=self.historical_data["value"],
                mode="lines",
                name="Historical",
                line=dict(color=config.COLORS["historical"], width=1.5),
                hovertemplate="<b>Historical</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>",
            )
        )

        self._fig.add_trace(
            go.Scatter(
                x=self.forecast_data["timestamp"],
                y=self.forecast_data["mean"],
                mode="lines",
                name="Forecast",
                line=dict(color=config.COLORS["forecast"], width=2),
                hovertemplate="<b>Forecast</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>",
            )
        )

        self._fig.add_trace(
            go.Scatter(
                x=self.actual_data["timestamp"],
                y=self.actual_data["value"],
                mode="lines+markers",
                name="Actual",
                line=dict(color=config.COLORS["actual"], width=2),
                marker=dict(size=4),
                hovertemplate="<b>Actual</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>",
            )
        )

        self._fig.add_shape(
            type="line",
            x0=self.forecast_start,
            x1=self.forecast_start,
            y0=0,
            y1=1,
            yref="paper",
            line=dict(color=config.COLORS["forecast_start"], width=2, dash="dash"),
        )

        self._fig.add_annotation(
            x=self.forecast_start,
            y=1,
            yref="paper",
            text="Forecast Start",
            showarrow=False,
            yshift=10,
        )

        plot_title = self.title or "Forecast vs Actual Values"
        if self.sensor_id:
            plot_title += f" - {self.sensor_id}"

        self._fig.update_layout(
            title=plot_title,
            xaxis_title="Time",
            yaxis_title="Value",
            hovermode="x unified",
            template="plotly_white",
            height=600,
            showlegend=True,
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


class ResidualPlotInteractive(PlotlyVisualizationInterface):
    """
    Create interactive Plotly residuals plot over time.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.forecasting import ResidualPlotInteractive

    plot = ResidualPlotInteractive(
        actual=actual_series,
        predicted=predicted_series,
        timestamps=timestamp_series,
        sensor_id='SENSOR_001'
    )
    fig = plot.plot()
    ```

    Parameters:
        actual (pd.Series): Actual values.
        predicted (pd.Series): Predicted values.
        timestamps (pd.Series): Timestamps for x-axis.
        sensor_id (str, optional): Sensor identifier for the plot title.
        title (str, optional): Custom plot title.
    """

    actual: pd.Series
    predicted: pd.Series
    timestamps: pd.Series
    sensor_id: Optional[str]
    title: Optional[str]
    _fig: Optional[go.Figure]

    def __init__(
        self,
        actual: pd.Series,
        predicted: pd.Series,
        timestamps: pd.Series,
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
    ) -> None:
        if actual is None or len(actual) == 0:
            raise VisualizationDataError(
                "actual cannot be None or empty. Please provide actual values."
            )
        if predicted is None or len(predicted) == 0:
            raise VisualizationDataError(
                "predicted cannot be None or empty. Please provide predicted values."
            )
        if timestamps is None or len(timestamps) == 0:
            raise VisualizationDataError(
                "timestamps cannot be None or empty. Please provide timestamps."
            )
        if len(actual) != len(predicted) or len(actual) != len(timestamps):
            raise VisualizationDataError(
                f"Length mismatch: actual ({len(actual)}), predicted ({len(predicted)}), "
                f"timestamps ({len(timestamps)}) must all have the same length."
            )

        self.actual = pd.to_numeric(actual, errors="coerce")
        self.predicted = pd.to_numeric(predicted, errors="coerce")
        self.timestamps = pd.to_datetime(timestamps, errors="coerce")
        self.sensor_id = sensor_id
        self.title = title
        self._fig = None

    def plot(self) -> go.Figure:
        """Generate the interactive residuals visualization."""
        residuals = self.actual - self.predicted

        self._fig = go.Figure()

        self._fig.add_trace(
            go.Scatter(
                x=self.timestamps,
                y=residuals,
                mode="lines+markers",
                name="Residuals",
                line=dict(color=config.COLORS["anomaly"], width=1.5),
                marker=dict(size=4),
                hovertemplate="<b>Residual</b><br>Time: %{x}<br>Error: %{y:.2f}<extra></extra>",
            )
        )

        self._fig.add_hline(
            y=0, line_dash="dash", line_color="gray", annotation_text="Zero Error"
        )

        plot_title = self.title or "Residuals Over Time"
        if self.sensor_id:
            plot_title += f" - {self.sensor_id}"

        self._fig.update_layout(
            title=plot_title,
            xaxis_title="Time",
            yaxis_title="Residual (Actual - Predicted)",
            hovermode="x unified",
            template="plotly_white",
            height=500,
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


class ErrorDistributionPlotInteractive(PlotlyVisualizationInterface):
    """
    Create interactive Plotly histogram of forecast errors.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.forecasting import ErrorDistributionPlotInteractive

    plot = ErrorDistributionPlotInteractive(
        actual=actual_series,
        predicted=predicted_series,
        sensor_id='SENSOR_001',
        bins=30
    )
    fig = plot.plot()
    ```

    Parameters:
        actual (pd.Series): Actual values.
        predicted (pd.Series): Predicted values.
        sensor_id (str, optional): Sensor identifier for the plot title.
        title (str, optional): Custom plot title.
        bins (int, optional): Number of histogram bins. Defaults to 30.
    """

    actual: pd.Series
    predicted: pd.Series
    sensor_id: Optional[str]
    title: Optional[str]
    bins: int
    _fig: Optional[go.Figure]

    def __init__(
        self,
        actual: pd.Series,
        predicted: pd.Series,
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        bins: int = 30,
    ) -> None:
        if actual is None or len(actual) == 0:
            raise VisualizationDataError(
                "actual cannot be None or empty. Please provide actual values."
            )
        if predicted is None or len(predicted) == 0:
            raise VisualizationDataError(
                "predicted cannot be None or empty. Please provide predicted values."
            )
        if len(actual) != len(predicted):
            raise VisualizationDataError(
                f"Length mismatch: actual ({len(actual)}) and predicted ({len(predicted)}) "
                f"must have the same length."
            )

        self.actual = pd.to_numeric(actual, errors="coerce")
        self.predicted = pd.to_numeric(predicted, errors="coerce")
        self.sensor_id = sensor_id
        self.title = title
        self.bins = bins
        self._fig = None

    def plot(self) -> go.Figure:
        """Generate the interactive error distribution visualization."""
        errors = self.actual - self.predicted

        self._fig = go.Figure()

        self._fig.add_trace(
            go.Histogram(
                x=errors,
                nbinsx=self.bins,
                name="Error Distribution",
                marker_color=config.COLORS["anomaly"],
                opacity=0.7,
                hovertemplate="Error: %{x:.2f}<br>Count: %{y}<extra></extra>",
            )
        )

        mean_error = errors.mean()
        self._fig.add_vline(
            x=mean_error,
            line_dash="dash",
            line_color="black",
            annotation_text=f"Mean: {mean_error:.2f}",
        )

        plot_title = self.title or "Forecast Error Distribution"
        if self.sensor_id:
            plot_title += f" - {self.sensor_id}"

        mae = np.abs(errors).mean()
        rmse = np.sqrt((errors**2).mean())

        self._fig.update_layout(
            title=plot_title,
            xaxis_title="Error (Actual - Predicted)",
            yaxis_title="Frequency",
            template="plotly_white",
            height=500,
            annotations=[
                dict(
                    x=0.98,
                    y=0.98,
                    xref="paper",
                    yref="paper",
                    text=f"MAE: {mae:.2f}<br>RMSE: {rmse:.2f}",
                    showarrow=False,
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="black",
                    borderwidth=1,
                )
            ],
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


class ScatterPlotInteractive(PlotlyVisualizationInterface):
    """
    Create interactive Plotly scatter plot of actual vs predicted values.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.forecasting import ScatterPlotInteractive

    plot = ScatterPlotInteractive(
        actual=actual_series,
        predicted=predicted_series,
        sensor_id='SENSOR_001'
    )
    fig = plot.plot()
    ```

    Parameters:
        actual (pd.Series): Actual values.
        predicted (pd.Series): Predicted values.
        sensor_id (str, optional): Sensor identifier for the plot title.
        title (str, optional): Custom plot title.
    """

    actual: pd.Series
    predicted: pd.Series
    sensor_id: Optional[str]
    title: Optional[str]
    _fig: Optional[go.Figure]

    def __init__(
        self,
        actual: pd.Series,
        predicted: pd.Series,
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
    ) -> None:
        if actual is None or len(actual) == 0:
            raise VisualizationDataError(
                "actual cannot be None or empty. Please provide actual values."
            )
        if predicted is None or len(predicted) == 0:
            raise VisualizationDataError(
                "predicted cannot be None or empty. Please provide predicted values."
            )
        if len(actual) != len(predicted):
            raise VisualizationDataError(
                f"Length mismatch: actual ({len(actual)}) and predicted ({len(predicted)}) "
                f"must have the same length."
            )

        self.actual = pd.to_numeric(actual, errors="coerce")
        self.predicted = pd.to_numeric(predicted, errors="coerce")
        self.sensor_id = sensor_id
        self.title = title
        self._fig = None

    def plot(self) -> go.Figure:
        """Generate the interactive scatter plot visualization."""
        self._fig = go.Figure()

        self._fig.add_trace(
            go.Scatter(
                x=self.actual,
                y=self.predicted,
                mode="markers",
                name="Predictions",
                marker=dict(color=config.COLORS["forecast"], size=8, opacity=0.6),
                hovertemplate="<b>Point</b><br>Actual: %{x:.2f}<br>Predicted: %{y:.2f}<extra></extra>",
            )
        )

        min_val = min(self.actual.min(), self.predicted.min())
        max_val = max(self.actual.max(), self.predicted.max())

        self._fig.add_trace(
            go.Scatter(
                x=[min_val, max_val],
                y=[min_val, max_val],
                mode="lines",
                name="Perfect Prediction",
                line=dict(color="gray", dash="dash", width=2),
                hoverinfo="skip",
            )
        )

        try:
            from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

            mae = mean_absolute_error(self.actual, self.predicted)
            rmse = np.sqrt(mean_squared_error(self.actual, self.predicted))
            r2 = r2_score(self.actual, self.predicted)
        except ImportError:
            errors = self.actual - self.predicted
            mae = np.abs(errors).mean()
            rmse = np.sqrt((errors**2).mean())
            # Calculate R² manually: 1 - SS_res/SS_tot
            ss_res = np.sum(errors**2)
            ss_tot = np.sum((self.actual - self.actual.mean()) ** 2)
            r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

        plot_title = self.title or "Actual vs Predicted Values"
        if self.sensor_id:
            plot_title += f" - {self.sensor_id}"

        self._fig.update_layout(
            title=plot_title,
            xaxis_title="Actual Value",
            yaxis_title="Predicted Value",
            template="plotly_white",
            height=600,
            annotations=[
                dict(
                    x=0.98,
                    y=0.02,
                    xref="paper",
                    yref="paper",
                    text=f"R²: {r2:.4f}<br>MAE: {mae:.2f}<br>RMSE: {rmse:.2f}",
                    showarrow=False,
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="black",
                    borderwidth=1,
                    align="left",
                )
            ],
        )

        self._fig.update_xaxes(scaleanchor="y", scaleratio=1)

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
