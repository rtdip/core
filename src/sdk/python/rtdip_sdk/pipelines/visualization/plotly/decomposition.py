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
Plotly-based interactive decomposition visualization components.

This module provides class-based interactive visualization components for
time series decomposition results using Plotly.

Example
--------
```python
from rtdip_sdk.pipelines.decomposition.pandas import STLDecomposition
from rtdip_sdk.pipelines.visualization.plotly.decomposition import DecompositionPlotInteractive

# Decompose time series
stl = STLDecomposition(df=data, value_column="value", timestamp_column="timestamp", period=7)
result = stl.decompose()

# Visualize interactively
plot = DecompositionPlotInteractive(decomposition_data=result, sensor_id="SENSOR_001")
fig = plot.plot()
plot.save("decomposition.html")
```
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from pandas import DataFrame as PandasDataFrame

from .. import config
from ..interfaces import PlotlyVisualizationInterface
from ..validation import (
    VisualizationDataError,
    apply_column_mapping,
    coerce_types,
    validate_dataframe,
)


def _get_seasonal_columns(df: PandasDataFrame) -> List[str]:
    """
    Get list of seasonal column names from a decomposition DataFrame.

    Args:
        df: Decomposition output DataFrame

    Returns:
        List of seasonal column names, sorted by period if applicable
    """
    seasonal_cols = []

    if "seasonal" in df.columns:
        seasonal_cols.append("seasonal")

    pattern = re.compile(r"^seasonal_(\d+)$")
    for col in df.columns:
        match = pattern.match(col)
        if match:
            seasonal_cols.append(col)

    seasonal_cols = sorted(
        seasonal_cols,
        key=lambda x: int(re.search(r"\d+", x).group()) if "_" in x else 0,
    )

    return seasonal_cols


def _extract_period_from_column(col_name: str) -> Optional[int]:
    """Extract period value from seasonal column name."""
    match = re.search(r"seasonal_(\d+)", col_name)
    if match:
        return int(match.group(1))
    return None


def _get_period_label(
    period: Optional[int], custom_labels: Optional[Dict[int, str]] = None
) -> str:
    """
    Get human-readable label for a period value.

    Args:
        period: Period value (e.g., 24, 168, 1440)
        custom_labels: Optional dictionary mapping period values to custom labels.
            Takes precedence over built-in labels.

    Returns:
        Human-readable label (e.g., "Daily", "Weekly")
    """
    if period is None:
        return "Seasonal"

    # Check custom labels first
    if custom_labels and period in custom_labels:
        return custom_labels[period]

    default_labels = {
        24: "Daily (24h)",
        168: "Weekly (168h)",
        8760: "Yearly",
        1440: "Daily (1440min)",
        10080: "Weekly (10080min)",
        7: "Weekly (7d)",
        365: "Yearly (365d)",
        366: "Yearly (366d)",
    }

    return default_labels.get(period, f"Period {period}")


class DecompositionPlotInteractive(PlotlyVisualizationInterface):
    """
    Interactive Plotly decomposition plot with zoom, pan, and hover.

    Creates an interactive multi-panel visualization showing the original
    signal and its decomposed components (trend, seasonal, residual).

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.decomposition import DecompositionPlotInteractive

    plot = DecompositionPlotInteractive(
        decomposition_data=result_df,
        sensor_id="SENSOR_001",
        period_labels={144: "Day", 1008: "Week"}  # Custom period names
    )
    fig = plot.plot()
    plot.save_html("decomposition.html")
    ```

    Parameters:
        decomposition_data: DataFrame with decomposition output.
        timestamp_column: Name of timestamp column (default: "timestamp")
        value_column: Name of original value column (default: "value")
        sensor_id: Optional sensor identifier for the plot title.
        title: Optional custom plot title.
        show_rangeslider: Whether to show range slider (default: True).
        column_mapping: Optional column name mapping.
        period_labels: Optional mapping from period values to custom display names.
            Example: {144: "Day", 1008: "Week"} maps period 144 to "Day".
    """

    decomposition_data: PandasDataFrame
    timestamp_column: str
    value_column: str
    sensor_id: Optional[str]
    title: Optional[str]
    show_rangeslider: bool
    column_mapping: Optional[Dict[str, str]]
    period_labels: Optional[Dict[int, str]]
    _fig: Optional[go.Figure]
    _seasonal_columns: List[str]

    def __init__(
        self,
        decomposition_data: PandasDataFrame,
        timestamp_column: str = "timestamp",
        value_column: str = "value",
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        show_rangeslider: bool = True,
        column_mapping: Optional[Dict[str, str]] = None,
        period_labels: Optional[Dict[int, str]] = None,
    ) -> None:
        self.timestamp_column = timestamp_column
        self.value_column = value_column
        self.sensor_id = sensor_id
        self.title = title
        self.show_rangeslider = show_rangeslider
        self.column_mapping = column_mapping
        self.period_labels = period_labels
        self._fig = None

        self.decomposition_data = apply_column_mapping(
            decomposition_data, column_mapping, inplace=False
        )

        required_cols = [timestamp_column, value_column, "trend", "residual"]
        validate_dataframe(
            self.decomposition_data,
            required_columns=required_cols,
            df_name="decomposition_data",
        )

        self._seasonal_columns = _get_seasonal_columns(self.decomposition_data)
        if not self._seasonal_columns:
            raise VisualizationDataError(
                "decomposition_data must contain at least one seasonal column."
            )

        self.decomposition_data = coerce_types(
            self.decomposition_data,
            datetime_cols=[timestamp_column],
            numeric_cols=[value_column, "trend", "residual"] + self._seasonal_columns,
            inplace=True,
        )

        self.decomposition_data = self.decomposition_data.sort_values(
            timestamp_column
        ).reset_index(drop=True)

    def plot(self) -> go.Figure:
        """
        Generate the interactive decomposition visualization.

        Returns:
            plotly.graph_objects.Figure: The generated interactive figure.
        """
        n_panels = 3 + len(self._seasonal_columns)

        subplot_titles = ["Original", "Trend"]
        for col in self._seasonal_columns:
            period = _extract_period_from_column(col)
            subplot_titles.append(_get_period_label(period, self.period_labels))
        subplot_titles.append("Residual")

        self._fig = make_subplots(
            rows=n_panels,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.05,
            subplot_titles=subplot_titles,
        )

        timestamps = self.decomposition_data[self.timestamp_column]
        panel_idx = 1

        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data[self.value_column],
                mode="lines",
                name="Original",
                line=dict(color=config.DECOMPOSITION_COLORS["original"], width=1.5),
                hovertemplate="<b>Original</b><br>Time: %{x}<br>Value: %{y:.4f}<extra></extra>",
            ),
            row=panel_idx,
            col=1,
        )
        panel_idx += 1

        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data["trend"],
                mode="lines",
                name="Trend",
                line=dict(color=config.DECOMPOSITION_COLORS["trend"], width=2),
                hovertemplate="<b>Trend</b><br>Time: %{x}<br>Value: %{y:.4f}<extra></extra>",
            ),
            row=panel_idx,
            col=1,
        )
        panel_idx += 1

        for idx, col in enumerate(self._seasonal_columns):
            period = _extract_period_from_column(col)
            color = (
                config.get_seasonal_color(period, idx)
                if period
                else config.DECOMPOSITION_COLORS["seasonal"]
            )
            label = _get_period_label(period, self.period_labels)

            self._fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=self.decomposition_data[col],
                    mode="lines",
                    name=label,
                    line=dict(color=color, width=1.5),
                    hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.4f}}<extra></extra>",
                ),
                row=panel_idx,
                col=1,
            )
            panel_idx += 1

        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data["residual"],
                mode="lines",
                name="Residual",
                line=dict(color=config.DECOMPOSITION_COLORS["residual"], width=1),
                opacity=0.7,
                hovertemplate="<b>Residual</b><br>Time: %{x}<br>Value: %{y:.4f}<extra></extra>",
            ),
            row=panel_idx,
            col=1,
        )

        plot_title = self.title
        if plot_title is None:
            if self.sensor_id:
                plot_title = f"Time Series Decomposition - {self.sensor_id}"
            else:
                plot_title = "Time Series Decomposition"

        height = 200 + n_panels * 150

        self._fig.update_layout(
            title=dict(text=plot_title, font=dict(size=16, color="#2C3E50")),
            height=height,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            hovermode="x unified",
            template="plotly_white",
        )

        if self.show_rangeslider:
            self._fig.update_xaxes(
                rangeslider=dict(visible=True, thickness=0.05),
                row=n_panels,
                col=1,
            )

        self._fig.update_xaxes(title_text="Time", row=n_panels, col=1)

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
            filepath (Union[str, Path]): Output file path.
            format (str): Output format ("html" or "png").
            **kwargs (Any): Additional options (width, height, scale for PNG).

        Returns:
            Path: Path to the saved file.
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


class MSTLDecompositionPlotInteractive(PlotlyVisualizationInterface):
    """
    Interactive MSTL decomposition plot with multiple seasonal components.

    Creates an interactive visualization with linked zoom across all panels
    and detailed hover information for each component.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.decomposition import MSTLDecompositionPlotInteractive

    plot = MSTLDecompositionPlotInteractive(
        decomposition_data=mstl_result,
        sensor_id="SENSOR_001",
        period_labels={144: "Day", 1008: "Week"}  # Custom period names
    )
    fig = plot.plot()
    plot.save_html("mstl_decomposition.html")
    ```

    Parameters:
        decomposition_data: DataFrame with MSTL output.
        timestamp_column: Name of timestamp column (default: "timestamp")
        value_column: Name of original value column (default: "value")
        sensor_id: Optional sensor identifier.
        title: Optional custom title.
        show_rangeslider: Whether to show range slider (default: True).
        column_mapping: Optional column name mapping.
        period_labels: Optional mapping from period values to custom display names.
            Example: {144: "Day", 1008: "Week"} maps period 144 to "Day".
    """

    decomposition_data: PandasDataFrame
    timestamp_column: str
    value_column: str
    sensor_id: Optional[str]
    title: Optional[str]
    show_rangeslider: bool
    column_mapping: Optional[Dict[str, str]]
    period_labels: Optional[Dict[int, str]]
    _fig: Optional[go.Figure]
    _seasonal_columns: List[str]

    def __init__(
        self,
        decomposition_data: PandasDataFrame,
        timestamp_column: str = "timestamp",
        value_column: str = "value",
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        show_rangeslider: bool = True,
        column_mapping: Optional[Dict[str, str]] = None,
        period_labels: Optional[Dict[int, str]] = None,
    ) -> None:
        self.timestamp_column = timestamp_column
        self.value_column = value_column
        self.sensor_id = sensor_id
        self.title = title
        self.show_rangeslider = show_rangeslider
        self.column_mapping = column_mapping
        self.period_labels = period_labels
        self._fig = None

        self.decomposition_data = apply_column_mapping(
            decomposition_data, column_mapping, inplace=False
        )

        required_cols = [timestamp_column, value_column, "trend", "residual"]
        validate_dataframe(
            self.decomposition_data,
            required_columns=required_cols,
            df_name="decomposition_data",
        )

        self._seasonal_columns = _get_seasonal_columns(self.decomposition_data)
        if not self._seasonal_columns:
            raise VisualizationDataError(
                "decomposition_data must contain at least one seasonal column."
            )

        self.decomposition_data = coerce_types(
            self.decomposition_data,
            datetime_cols=[timestamp_column],
            numeric_cols=[value_column, "trend", "residual"] + self._seasonal_columns,
            inplace=True,
        )

        self.decomposition_data = self.decomposition_data.sort_values(
            timestamp_column
        ).reset_index(drop=True)

    def plot(self) -> go.Figure:
        """
        Generate the interactive MSTL decomposition visualization.

        Returns:
            plotly.graph_objects.Figure: The generated interactive figure.
        """
        n_seasonal = len(self._seasonal_columns)
        n_panels = 3 + n_seasonal

        subplot_titles = ["Original", "Trend"]
        for col in self._seasonal_columns:
            period = _extract_period_from_column(col)
            subplot_titles.append(_get_period_label(period, self.period_labels))
        subplot_titles.append("Residual")

        self._fig = make_subplots(
            rows=n_panels,
            cols=1,
            shared_xaxes=True,
            vertical_spacing=0.04,
            subplot_titles=subplot_titles,
        )

        timestamps = self.decomposition_data[self.timestamp_column]
        panel_idx = 1

        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data[self.value_column],
                mode="lines",
                name="Original",
                line=dict(color=config.DECOMPOSITION_COLORS["original"], width=1.5),
                hovertemplate="<b>Original</b><br>Time: %{x}<br>Value: %{y:.4f}<extra></extra>",
            ),
            row=panel_idx,
            col=1,
        )
        panel_idx += 1

        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data["trend"],
                mode="lines",
                name="Trend",
                line=dict(color=config.DECOMPOSITION_COLORS["trend"], width=2),
                hovertemplate="<b>Trend</b><br>Time: %{x}<br>Value: %{y:.4f}<extra></extra>",
            ),
            row=panel_idx,
            col=1,
        )
        panel_idx += 1

        for idx, col in enumerate(self._seasonal_columns):
            period = _extract_period_from_column(col)
            color = (
                config.get_seasonal_color(period, idx)
                if period
                else config.DECOMPOSITION_COLORS["seasonal"]
            )
            label = _get_period_label(period, self.period_labels)

            self._fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=self.decomposition_data[col],
                    mode="lines",
                    name=label,
                    line=dict(color=color, width=1.5),
                    hovertemplate=f"<b>{label}</b><br>Time: %{{x}}<br>Value: %{{y:.4f}}<extra></extra>",
                ),
                row=panel_idx,
                col=1,
            )
            panel_idx += 1

        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data["residual"],
                mode="lines",
                name="Residual",
                line=dict(color=config.DECOMPOSITION_COLORS["residual"], width=1),
                opacity=0.7,
                hovertemplate="<b>Residual</b><br>Time: %{x}<br>Value: %{y:.4f}<extra></extra>",
            ),
            row=panel_idx,
            col=1,
        )

        plot_title = self.title
        if plot_title is None:
            pattern_str = (
                f"{n_seasonal} seasonal pattern{'s' if n_seasonal > 1 else ''}"
            )
            if self.sensor_id:
                plot_title = f"MSTL Decomposition ({pattern_str}) - {self.sensor_id}"
            else:
                plot_title = f"MSTL Decomposition ({pattern_str})"

        height = 200 + n_panels * 140

        self._fig.update_layout(
            title=dict(text=plot_title, font=dict(size=16, color="#2C3E50")),
            height=height,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            hovermode="x unified",
            template="plotly_white",
        )

        if self.show_rangeslider:
            self._fig.update_xaxes(
                rangeslider=dict(visible=True, thickness=0.05),
                row=n_panels,
                col=1,
            )

        self._fig.update_xaxes(title_text="Time", row=n_panels, col=1)

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
            filepath (Union[str, Path]): Output file path.
            format (str): Output format ("html" or "png").
            **kwargs (Any): Additional options.

        Returns:
            Path: Path to the saved file.
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
                height=kwargs.get("height", 1000),
                scale=kwargs.get("scale", 2),
            )
        else:
            raise ValueError(f"Unsupported format: {format}")

        print(f"Saved: {filepath}")
        return filepath


class DecompositionDashboardInteractive(PlotlyVisualizationInterface):
    """
    Interactive decomposition dashboard with statistics.

    Creates a comprehensive interactive dashboard showing decomposition
    components alongside statistical analysis.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.plotly.decomposition import DecompositionDashboardInteractive

    dashboard = DecompositionDashboardInteractive(
        decomposition_data=result_df,
        sensor_id="SENSOR_001",
        period_labels={144: "Day", 1008: "Week"}  # Custom period names
    )
    fig = dashboard.plot()
    dashboard.save_html("decomposition_dashboard.html")
    ```

    Parameters:
        decomposition_data: DataFrame with decomposition output.
        timestamp_column: Name of timestamp column (default: "timestamp")
        value_column: Name of original value column (default: "value")
        sensor_id: Optional sensor identifier.
        title: Optional custom title.
        column_mapping: Optional column name mapping.
        period_labels: Optional mapping from period values to custom display names.
            Example: {144: "Day", 1008: "Week"} maps period 144 to "Day".
    """

    decomposition_data: PandasDataFrame
    timestamp_column: str
    value_column: str
    sensor_id: Optional[str]
    title: Optional[str]
    column_mapping: Optional[Dict[str, str]]
    period_labels: Optional[Dict[int, str]]
    _fig: Optional[go.Figure]
    _seasonal_columns: List[str]
    _statistics: Optional[Dict[str, Any]]

    def __init__(
        self,
        decomposition_data: PandasDataFrame,
        timestamp_column: str = "timestamp",
        value_column: str = "value",
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        period_labels: Optional[Dict[int, str]] = None,
    ) -> None:
        self.timestamp_column = timestamp_column
        self.value_column = value_column
        self.sensor_id = sensor_id
        self.title = title
        self.column_mapping = column_mapping
        self.period_labels = period_labels
        self._fig = None
        self._statistics = None

        self.decomposition_data = apply_column_mapping(
            decomposition_data, column_mapping, inplace=False
        )

        required_cols = [timestamp_column, value_column, "trend", "residual"]
        validate_dataframe(
            self.decomposition_data,
            required_columns=required_cols,
            df_name="decomposition_data",
        )

        self._seasonal_columns = _get_seasonal_columns(self.decomposition_data)
        if not self._seasonal_columns:
            raise VisualizationDataError(
                "decomposition_data must contain at least one seasonal column."
            )

        self.decomposition_data = coerce_types(
            self.decomposition_data,
            datetime_cols=[timestamp_column],
            numeric_cols=[value_column, "trend", "residual"] + self._seasonal_columns,
            inplace=True,
        )

        self.decomposition_data = self.decomposition_data.sort_values(
            timestamp_column
        ).reset_index(drop=True)

    def _calculate_statistics(self) -> Dict[str, Any]:
        """Calculate decomposition statistics."""
        df = self.decomposition_data
        total_var = df[self.value_column].var()

        if total_var == 0:
            total_var = 1e-10

        stats: Dict[str, Any] = {
            "variance_explained": {},
            "seasonality_strength": {},
            "residual_diagnostics": {},
        }

        trend_var = df["trend"].dropna().var()
        stats["variance_explained"]["trend"] = (trend_var / total_var) * 100

        residual_var = df["residual"].dropna().var()
        stats["variance_explained"]["residual"] = (residual_var / total_var) * 100

        for col in self._seasonal_columns:
            seasonal_var = df[col].dropna().var()
            stats["variance_explained"][col] = (seasonal_var / total_var) * 100

            seasonal_plus_resid = df[col] + df["residual"]
            spr_var = seasonal_plus_resid.dropna().var()
            if spr_var > 0:
                strength = max(0, 1 - residual_var / spr_var)
            else:
                strength = 0
            stats["seasonality_strength"][col] = strength

        residuals = df["residual"].dropna()
        stats["residual_diagnostics"] = {
            "mean": residuals.mean(),
            "std": residuals.std(),
            "skewness": residuals.skew(),
            "kurtosis": residuals.kurtosis(),
        }

        return stats

    def get_statistics(self) -> Dict[str, Any]:
        """Get calculated statistics."""
        if self._statistics is None:
            self._statistics = self._calculate_statistics()
        return self._statistics

    def plot(self) -> go.Figure:
        """
        Generate the interactive decomposition dashboard.

        Returns:
            plotly.graph_objects.Figure: The generated interactive figure.
        """
        self._statistics = self._calculate_statistics()

        n_seasonal = len(self._seasonal_columns)

        self._fig = make_subplots(
            rows=3,
            cols=2,
            specs=[
                [{"type": "scatter"}, {"type": "scatter"}],
                [{"type": "scatter", "colspan": 2}, None],
                [{"type": "scatter"}, {"type": "table"}],
            ],
            subplot_titles=[
                "Original Signal",
                "Trend Component",
                "Seasonal Components",
                "Residual",
                "Statistics",
            ],
            vertical_spacing=0.1,
            horizontal_spacing=0.08,
        )

        timestamps = self.decomposition_data[self.timestamp_column]

        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data[self.value_column],
                mode="lines",
                name="Original",
                line=dict(color=config.DECOMPOSITION_COLORS["original"], width=1.5),
                hovertemplate="<b>Original</b><br>%{x}<br>%{y:.4f}<extra></extra>",
            ),
            row=1,
            col=1,
        )

        trend_var = self._statistics["variance_explained"]["trend"]
        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data["trend"],
                mode="lines",
                name=f"Trend ({trend_var:.1f}%)",
                line=dict(color=config.DECOMPOSITION_COLORS["trend"], width=2),
                hovertemplate="<b>Trend</b><br>%{x}<br>%{y:.4f}<extra></extra>",
            ),
            row=1,
            col=2,
        )

        for idx, col in enumerate(self._seasonal_columns):
            period = _extract_period_from_column(col)
            color = (
                config.get_seasonal_color(period, idx)
                if period
                else config.DECOMPOSITION_COLORS["seasonal"]
            )
            label = _get_period_label(period, self.period_labels)
            strength = self._statistics["seasonality_strength"].get(col, 0)

            self._fig.add_trace(
                go.Scatter(
                    x=timestamps,
                    y=self.decomposition_data[col],
                    mode="lines",
                    name=f"{label} (str: {strength:.2f})",
                    line=dict(color=color, width=1.5),
                    hovertemplate=f"<b>{label}</b><br>%{{x}}<br>%{{y:.4f}}<extra></extra>",
                ),
                row=2,
                col=1,
            )

        resid_var = self._statistics["variance_explained"]["residual"]
        self._fig.add_trace(
            go.Scatter(
                x=timestamps,
                y=self.decomposition_data["residual"],
                mode="lines",
                name=f"Residual ({resid_var:.1f}%)",
                line=dict(color=config.DECOMPOSITION_COLORS["residual"], width=1),
                opacity=0.7,
                hovertemplate="<b>Residual</b><br>%{x}<br>%{y:.4f}<extra></extra>",
            ),
            row=3,
            col=1,
        )

        header_values = ["Component", "Variance %", "Strength"]
        cell_values = [[], [], []]

        cell_values[0].append("Trend")
        cell_values[1].append(f"{self._statistics['variance_explained']['trend']:.1f}%")
        cell_values[2].append("-")

        for col in self._seasonal_columns:
            period = _extract_period_from_column(col)
            label = (
                _get_period_label(period, self.period_labels) if period else "Seasonal"
            )
            var_pct = self._statistics["variance_explained"].get(col, 0)
            strength = self._statistics["seasonality_strength"].get(col, 0)
            cell_values[0].append(label)
            cell_values[1].append(f"{var_pct:.1f}%")
            cell_values[2].append(f"{strength:.3f}")

        cell_values[0].append("Residual")
        cell_values[1].append(
            f"{self._statistics['variance_explained']['residual']:.1f}%"
        )
        cell_values[2].append("-")

        cell_values[0].append("")
        cell_values[1].append("")
        cell_values[2].append("")

        diag = self._statistics["residual_diagnostics"]
        cell_values[0].append("Residual Mean")
        cell_values[1].append(f"{diag['mean']:.4f}")
        cell_values[2].append("")

        cell_values[0].append("Residual Std")
        cell_values[1].append(f"{diag['std']:.4f}")
        cell_values[2].append("")

        cell_values[0].append("Skewness")
        cell_values[1].append(f"{diag['skewness']:.3f}")
        cell_values[2].append("")

        cell_values[0].append("Kurtosis")
        cell_values[1].append(f"{diag['kurtosis']:.3f}")
        cell_values[2].append("")

        self._fig.add_trace(
            go.Table(
                header=dict(
                    values=header_values,
                    fill_color="#2C3E50",
                    font=dict(color="white", size=12),
                    align="center",
                ),
                cells=dict(
                    values=cell_values,
                    fill_color=[
                        ["white"] * len(cell_values[0]),
                        ["white"] * len(cell_values[1]),
                        ["white"] * len(cell_values[2]),
                    ],
                    font=dict(size=11),
                    align="center",
                    height=25,
                ),
            ),
            row=3,
            col=2,
        )

        plot_title = self.title
        if plot_title is None:
            if self.sensor_id:
                plot_title = f"Decomposition Dashboard - {self.sensor_id}"
            else:
                plot_title = "Decomposition Dashboard"

        self._fig.update_layout(
            title=dict(text=plot_title, font=dict(size=18, color="#2C3E50")),
            height=900,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
            ),
            hovermode="x unified",
            template="plotly_white",
        )

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        format: str = "html",
        **kwargs,
    ) -> Path:
        """
        Save the dashboard to file.

        Args:
            filepath (Union[str, Path]): Output file path.
            format (str): Output format ("html" or "png").
            **kwargs (Any): Additional options.

        Returns:
            Path: Path to the saved file.
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
                width=kwargs.get("width", 1400),
                height=kwargs.get("height", 900),
                scale=kwargs.get("scale", 2),
            )
        else:
            raise ValueError(f"Unsupported format: {format}")

        print(f"Saved: {filepath}")
        return filepath
