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
Matplotlib-based decomposition visualization components.

This module provides class-based visualization components for time series
decomposition results, including STL, Classical, and MSTL decomposition outputs.

Example
--------
```python
from rtdip_sdk.pipelines.decomposition.pandas import STLDecomposition
from rtdip_sdk.pipelines.visualization.matplotlib.decomposition import DecompositionPlot

# Decompose time series
stl = STLDecomposition(df=data, value_column="value", timestamp_column="timestamp", period=7)
result = stl.decompose()

# Visualize decomposition
plot = DecompositionPlot(decomposition_data=result, sensor_id="SENSOR_001")
fig = plot.plot()
plot.save("decomposition.png")
```
"""

import re
import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pandas import DataFrame as PandasDataFrame

from .. import config
from .. import utils
from ..interfaces import MatplotlibVisualizationInterface
from ..validation import (
    VisualizationDataError,
    apply_column_mapping,
    coerce_types,
    prepare_dataframe,
    validate_dataframe,
)

warnings.filterwarnings("ignore")


def _get_seasonal_columns(df: PandasDataFrame) -> List[str]:
    """
    Get list of seasonal column names from a decomposition DataFrame.

    Detects both single seasonal ("seasonal") and multiple seasonal
    columns ("seasonal_24", "seasonal_168", etc.).

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
    """
    Extract period value from seasonal column name.

    Args:
        col_name: Column name like "seasonal_24" or "seasonal"

    Returns:
        Period as integer, or None if not found
    """
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


class DecompositionPlot(MatplotlibVisualizationInterface):
    """
    Plot time series decomposition results (Original, Trend, Seasonal, Residual).

    Creates a 4-panel visualization showing the original signal and its
    decomposed components. Supports output from STL and Classical decomposition.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.decomposition import DecompositionPlot

    plot = DecompositionPlot(
        decomposition_data=result_df,
        sensor_id="SENSOR_001",
        title="STL Decomposition Results",
        period_labels={144: "Day", 1008: "Week"}  # Custom period names
    )
    fig = plot.plot()
    plot.save("decomposition.png")
    ```

    Parameters:
        decomposition_data (PandasDataFrame): DataFrame with decomposition output containing
            timestamp, value, trend, seasonal, and residual columns.
        sensor_id (Optional[str]): Optional sensor identifier for the plot title.
        title (Optional[str]): Optional custom plot title.
        show_legend (bool): Whether to show legends on each panel (default: True).
        column_mapping (Optional[Dict[str, str]]): Optional mapping from user column names to expected names.
        period_labels (Optional[Dict[int, str]]): Optional mapping from period values to custom display names.
            Example: {144: "Day", 1008: "Week"} maps period 144 to "Day".
    """

    decomposition_data: PandasDataFrame
    sensor_id: Optional[str]
    title: Optional[str]
    show_legend: bool
    column_mapping: Optional[Dict[str, str]]
    period_labels: Optional[Dict[int, str]]
    timestamp_column: str
    value_column: str
    _fig: Optional[plt.Figure]
    _axes: Optional[np.ndarray]
    _seasonal_columns: List[str]

    def __init__(
        self,
        decomposition_data: PandasDataFrame,
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        show_legend: bool = True,
        column_mapping: Optional[Dict[str, str]] = None,
        period_labels: Optional[Dict[int, str]] = None,
    ) -> None:
        self.sensor_id = sensor_id
        self.title = title
        self.show_legend = show_legend
        self.column_mapping = column_mapping
        self.period_labels = period_labels
        self.timestamp_column = "timestamp"
        self.value_column = "value"
        self._fig = None
        self._axes = None

        self.decomposition_data = apply_column_mapping(
            decomposition_data, column_mapping, inplace=False
        )

        required_cols = ["timestamp", "value", "trend", "residual"]
        validate_dataframe(
            self.decomposition_data,
            required_columns=required_cols,
            df_name="decomposition_data",
        )

        self._seasonal_columns = _get_seasonal_columns(self.decomposition_data)
        if not self._seasonal_columns:
            raise VisualizationDataError(
                "decomposition_data must contain at least one seasonal column "
                "('seasonal' or 'seasonal_N'). "
                f"Available columns: {list(self.decomposition_data.columns)}"
            )

        self.decomposition_data = coerce_types(
            self.decomposition_data,
            datetime_cols=["timestamp"],
            numeric_cols=["value", "trend", "residual"] + self._seasonal_columns,
            inplace=True,
        )

        self.decomposition_data = self.decomposition_data.sort_values(
            "timestamp"
        ).reset_index(drop=True)

    def plot(self, axes: Optional[np.ndarray] = None) -> plt.Figure:
        """
        Generate the decomposition visualization.

        Args:
            axes: Optional array of matplotlib axes to plot on.
                  If None, creates new figure with 4 subplots.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        utils.setup_plot_style()

        n_panels = 3 + len(self._seasonal_columns)
        figsize = config.get_decomposition_figsize(len(self._seasonal_columns))

        if axes is None:
            self._fig, self._axes = plt.subplots(
                n_panels, 1, figsize=figsize, sharex=True
            )
        else:
            self._axes = axes
            self._fig = axes[0].figure

        timestamps = self.decomposition_data[self.timestamp_column]
        panel_idx = 0

        self._axes[panel_idx].plot(
            timestamps,
            self.decomposition_data[self.value_column],
            color=config.DECOMPOSITION_COLORS["original"],
            linewidth=config.LINE_SETTINGS["linewidth"],
            label="Original",
        )
        self._axes[panel_idx].set_ylabel("Original")
        if self.show_legend:
            self._axes[panel_idx].legend(loc="upper right")
        utils.add_grid(self._axes[panel_idx])
        panel_idx += 1

        self._axes[panel_idx].plot(
            timestamps,
            self.decomposition_data["trend"],
            color=config.DECOMPOSITION_COLORS["trend"],
            linewidth=config.LINE_SETTINGS["linewidth"],
            label="Trend",
        )
        self._axes[panel_idx].set_ylabel("Trend")
        if self.show_legend:
            self._axes[panel_idx].legend(loc="upper right")
        utils.add_grid(self._axes[panel_idx])
        panel_idx += 1

        for idx, seasonal_col in enumerate(self._seasonal_columns):
            period = _extract_period_from_column(seasonal_col)
            color = (
                config.get_seasonal_color(period, idx)
                if period
                else config.DECOMPOSITION_COLORS["seasonal"]
            )
            label = _get_period_label(period, self.period_labels)

            self._axes[panel_idx].plot(
                timestamps,
                self.decomposition_data[seasonal_col],
                color=color,
                linewidth=config.LINE_SETTINGS["linewidth"],
                label=label,
            )
            self._axes[panel_idx].set_ylabel(label if period else "Seasonal")
            if self.show_legend:
                self._axes[panel_idx].legend(loc="upper right")
            utils.add_grid(self._axes[panel_idx])
            panel_idx += 1

        self._axes[panel_idx].plot(
            timestamps,
            self.decomposition_data["residual"],
            color=config.DECOMPOSITION_COLORS["residual"],
            linewidth=config.LINE_SETTINGS["linewidth_thin"],
            alpha=0.7,
            label="Residual",
        )
        self._axes[panel_idx].set_ylabel("Residual")
        self._axes[panel_idx].set_xlabel("Time")
        if self.show_legend:
            self._axes[panel_idx].legend(loc="upper right")
        utils.add_grid(self._axes[panel_idx])

        utils.format_time_axis(self._axes[-1])

        plot_title = self.title
        if plot_title is None:
            if self.sensor_id:
                plot_title = f"Time Series Decomposition - {self.sensor_id}"
            else:
                plot_title = "Time Series Decomposition"

        self._fig.suptitle(
            plot_title,
            fontsize=config.FONT_SIZES["title"] + 2,
            fontweight="bold",
            y=0.98,
        )

        self._fig.subplots_adjust(top=0.94, hspace=0.3, left=0.1, right=0.95)

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """
        Save the visualization to file.

        Args:
            filepath (Union[str, Path]): Output file path.
            dpi (Optional[int]): DPI for output image. If None, uses config default.
            **kwargs (Any): Additional options passed to utils.save_plot.

        Returns:
            Path: Path to the saved file.
        """
        if self._fig is None:
            self.plot()

        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )


class MSTLDecompositionPlot(MatplotlibVisualizationInterface):
    """
    Plot MSTL decomposition results with multiple seasonal components.

    Dynamically creates panels based on the number of seasonal components
    detected in the input data. Supports zooming into specific time ranges
    for seasonal panels to better visualize periodic patterns.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.decomposition import MSTLDecompositionPlot

    plot = MSTLDecompositionPlot(
        decomposition_data=mstl_result,
        sensor_id="SENSOR_001",
        zoom_periods={"seasonal_24": 168},  # Show 1 week of daily pattern
        period_labels={144: "Day", 1008: "Week"}  # Custom period names
    )
    fig = plot.plot()
    plot.save("mstl_decomposition.png")
    ```

    Parameters:
        decomposition_data: DataFrame with MSTL output containing timestamp,
            value, trend, seasonal_* columns, and residual.
        timestamp_column: Name of timestamp column (default: "timestamp")
        value_column: Name of original value column (default: "value")
        sensor_id: Optional sensor identifier for the plot title.
        title: Optional custom plot title.
        zoom_periods: Dict mapping seasonal column names to number of points
            to display (e.g., {"seasonal_24": 168} shows 1 week of daily pattern).
        show_legend: Whether to show legends (default: True).
        column_mapping: Optional column name mapping.
        period_labels: Optional mapping from period values to custom display names.
            Example: {144: "Day", 1008: "Week"} maps period 144 to "Day".
    """

    decomposition_data: PandasDataFrame
    timestamp_column: str
    value_column: str
    sensor_id: Optional[str]
    title: Optional[str]
    zoom_periods: Optional[Dict[str, int]]
    show_legend: bool
    column_mapping: Optional[Dict[str, str]]
    period_labels: Optional[Dict[int, str]]
    _fig: Optional[plt.Figure]
    _axes: Optional[np.ndarray]
    _seasonal_columns: List[str]

    def __init__(
        self,
        decomposition_data: PandasDataFrame,
        timestamp_column: str = "timestamp",
        value_column: str = "value",
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        zoom_periods: Optional[Dict[str, int]] = None,
        show_legend: bool = True,
        column_mapping: Optional[Dict[str, str]] = None,
        period_labels: Optional[Dict[int, str]] = None,
    ) -> None:
        self.timestamp_column = timestamp_column
        self.value_column = value_column
        self.sensor_id = sensor_id
        self.title = title
        self.zoom_periods = zoom_periods or {}
        self.show_legend = show_legend
        self.column_mapping = column_mapping
        self.period_labels = period_labels
        self._fig = None
        self._axes = None

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
                "decomposition_data must contain at least one seasonal column. "
                f"Available columns: {list(self.decomposition_data.columns)}"
            )

        self.decomposition_data = coerce_types(
            self.decomposition_data,
            datetime_cols=[timestamp_column],
            numeric_cols=[value_column, "trend", "residual"] + self._seasonal_columns,
            inplace=True,
        )

        self.decomposition_data = self.decomposition_data.sort_values(
            "timestamp"
        ).reset_index(drop=True)

    def plot(self, axes: Optional[np.ndarray] = None) -> plt.Figure:
        """
        Generate the MSTL decomposition visualization.

        Args:
            axes: Optional array of matplotlib axes. If None, creates new figure.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        utils.setup_plot_style()

        n_seasonal = len(self._seasonal_columns)
        n_panels = 3 + n_seasonal
        figsize = config.get_decomposition_figsize(n_seasonal)

        if axes is None:
            self._fig, self._axes = plt.subplots(
                n_panels, 1, figsize=figsize, sharex=False
            )
        else:
            self._axes = axes
            self._fig = axes[0].figure

        timestamps = self.decomposition_data[self.timestamp_column]
        values = self.decomposition_data[self.value_column]
        panel_idx = 0

        self._axes[panel_idx].plot(
            timestamps,
            values,
            color=config.DECOMPOSITION_COLORS["original"],
            linewidth=config.LINE_SETTINGS["linewidth"],
            label="Original",
        )
        self._axes[panel_idx].set_ylabel("Original")
        if self.show_legend:
            self._axes[panel_idx].legend(loc="upper right")
        utils.add_grid(self._axes[panel_idx])
        panel_idx += 1

        self._axes[panel_idx].plot(
            timestamps,
            self.decomposition_data["trend"],
            color=config.DECOMPOSITION_COLORS["trend"],
            linewidth=config.LINE_SETTINGS["linewidth"],
            label="Trend",
        )
        self._axes[panel_idx].set_ylabel("Trend")
        if self.show_legend:
            self._axes[panel_idx].legend(loc="upper right")
        utils.add_grid(self._axes[panel_idx])
        panel_idx += 1

        for idx, seasonal_col in enumerate(self._seasonal_columns):
            period = _extract_period_from_column(seasonal_col)
            color = (
                config.get_seasonal_color(period, idx)
                if period
                else config.DECOMPOSITION_COLORS["seasonal"]
            )
            label = _get_period_label(period, self.period_labels)

            zoom_n = self.zoom_periods.get(seasonal_col)
            if zoom_n and zoom_n < len(self.decomposition_data):
                plot_ts = timestamps[:zoom_n]
                plot_vals = self.decomposition_data[seasonal_col][:zoom_n]
                label += " (zoomed)"
            else:
                plot_ts = timestamps
                plot_vals = self.decomposition_data[seasonal_col]

            self._axes[panel_idx].plot(
                plot_ts,
                plot_vals,
                color=color,
                linewidth=config.LINE_SETTINGS["linewidth"],
                label=label,
            )
            self._axes[panel_idx].set_ylabel(label.replace(" (zoomed)", ""))
            if self.show_legend:
                self._axes[panel_idx].legend(loc="upper right")
            utils.add_grid(self._axes[panel_idx])
            utils.format_time_axis(self._axes[panel_idx])
            panel_idx += 1

        self._axes[panel_idx].plot(
            timestamps,
            self.decomposition_data["residual"],
            color=config.DECOMPOSITION_COLORS["residual"],
            linewidth=config.LINE_SETTINGS["linewidth_thin"],
            alpha=0.7,
            label="Residual",
        )
        self._axes[panel_idx].set_ylabel("Residual")
        self._axes[panel_idx].set_xlabel("Time")
        if self.show_legend:
            self._axes[panel_idx].legend(loc="upper right")
        utils.add_grid(self._axes[panel_idx])
        utils.format_time_axis(self._axes[panel_idx])

        plot_title = self.title
        if plot_title is None:
            n_patterns = len(self._seasonal_columns)
            pattern_str = (
                f"{n_patterns} seasonal pattern{'s' if n_patterns > 1 else ''}"
            )
            if self.sensor_id:
                plot_title = f"MSTL Decomposition ({pattern_str}) - {self.sensor_id}"
            else:
                plot_title = f"MSTL Decomposition ({pattern_str})"

        self._fig.suptitle(
            plot_title,
            fontsize=config.FONT_SIZES["title"] + 2,
            fontweight="bold",
            y=0.98,
        )

        self._fig.subplots_adjust(top=0.94, hspace=0.3, left=0.1, right=0.95)

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """
        Save the visualization to file.

        Args:
            filepath (Union[str, Path]): Output file path.
            dpi (Optional[int]): DPI for output image.
            **kwargs (Any): Additional save options.

        Returns:
            Path: Path to the saved file.
        """
        if self._fig is None:
            self.plot()

        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )


class DecompositionDashboard(MatplotlibVisualizationInterface):
    """
    Comprehensive decomposition dashboard with statistics.

    Creates a multi-panel visualization showing decomposition components
    along with statistical analysis including variance explained by each
    component, seasonality strength, and residual diagnostics.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.decomposition import DecompositionDashboard

    dashboard = DecompositionDashboard(
        decomposition_data=result_df,
        sensor_id="SENSOR_001",
        period_labels={144: "Day", 1008: "Week"}  # Custom period names
    )
    fig = dashboard.plot()
    dashboard.save("decomposition_dashboard.png")
    ```

    Parameters:
        decomposition_data: DataFrame with decomposition output.
        timestamp_column: Name of timestamp column (default: "timestamp")
        value_column: Name of original value column (default: "value")
        sensor_id: Optional sensor identifier.
        title: Optional custom title.
        show_statistics: Whether to show statistics panel (default: True).
        column_mapping: Optional column name mapping.
        period_labels: Optional mapping from period values to custom display names.
            Example: {144: "Day", 1008: "Week"} maps period 144 to "Day".
    """

    decomposition_data: PandasDataFrame
    timestamp_column: str
    value_column: str
    sensor_id: Optional[str]
    title: Optional[str]
    show_statistics: bool
    column_mapping: Optional[Dict[str, str]]
    period_labels: Optional[Dict[int, str]]
    _fig: Optional[plt.Figure]
    _seasonal_columns: List[str]
    _statistics: Optional[Dict[str, Any]]

    def __init__(
        self,
        decomposition_data: PandasDataFrame,
        timestamp_column: str = "timestamp",
        value_column: str = "value",
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        show_statistics: bool = True,
        column_mapping: Optional[Dict[str, str]] = None,
        period_labels: Optional[Dict[int, str]] = None,
    ) -> None:
        self.timestamp_column = timestamp_column
        self.value_column = value_column
        self.sensor_id = sensor_id
        self.title = title
        self.show_statistics = show_statistics
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
            "timestamp"
        ).reset_index(drop=True)

    def _calculate_statistics(self) -> Dict[str, Any]:
        """
        Calculate decomposition statistics.

        Returns:
            Dictionary containing variance explained, seasonality strength,
            and residual diagnostics.
        """
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
        """
        Get calculated statistics.

        Returns:
            Dictionary with variance explained, seasonality strength,
            and residual diagnostics.
        """
        if self._statistics is None:
            self._statistics = self._calculate_statistics()
        return self._statistics

    def plot(self) -> plt.Figure:
        """
        Generate the decomposition dashboard.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        utils.setup_plot_style()

        self._statistics = self._calculate_statistics()

        n_seasonal = len(self._seasonal_columns)
        if self.show_statistics:
            self._fig = plt.figure(figsize=config.FIGSIZE["decomposition_dashboard"])
            gs = self._fig.add_gridspec(3, 2, hspace=0.35, wspace=0.25)

            ax_original = self._fig.add_subplot(gs[0, 0])
            ax_trend = self._fig.add_subplot(gs[0, 1])
            ax_seasonal = self._fig.add_subplot(gs[1, :])
            ax_residual = self._fig.add_subplot(gs[2, 0])
            ax_stats = self._fig.add_subplot(gs[2, 1])
        else:
            figsize = config.get_decomposition_figsize(n_seasonal)
            self._fig, axes = plt.subplots(4, 1, figsize=figsize, sharex=True)
            ax_original, ax_trend, ax_seasonal, ax_residual = axes
            ax_stats = None

        timestamps = self.decomposition_data[self.timestamp_column]

        ax_original.plot(
            timestamps,
            self.decomposition_data[self.value_column],
            color=config.DECOMPOSITION_COLORS["original"],
            linewidth=config.LINE_SETTINGS["linewidth"],
        )
        ax_original.set_ylabel("Original")
        ax_original.set_title("Original Signal", fontweight="bold")
        utils.add_grid(ax_original)
        utils.format_time_axis(ax_original)

        ax_trend.plot(
            timestamps,
            self.decomposition_data["trend"],
            color=config.DECOMPOSITION_COLORS["trend"],
            linewidth=config.LINE_SETTINGS["linewidth"],
        )
        ax_trend.set_ylabel("Trend")
        trend_var = self._statistics["variance_explained"]["trend"]
        ax_trend.set_title(f"Trend ({trend_var:.1f}% variance)", fontweight="bold")
        utils.add_grid(ax_trend)
        utils.format_time_axis(ax_trend)

        for idx, col in enumerate(self._seasonal_columns):
            period = _extract_period_from_column(col)
            color = (
                config.get_seasonal_color(period, idx)
                if period
                else config.DECOMPOSITION_COLORS["seasonal"]
            )
            label = _get_period_label(period, self.period_labels)
            strength = self._statistics["seasonality_strength"].get(col, 0)

            ax_seasonal.plot(
                timestamps,
                self.decomposition_data[col],
                color=color,
                linewidth=config.LINE_SETTINGS["linewidth"],
                label=f"{label} (strength: {strength:.2f})",
            )

        ax_seasonal.set_ylabel("Seasonal")
        total_seasonal_var = sum(
            self._statistics["variance_explained"].get(col, 0)
            for col in self._seasonal_columns
        )
        ax_seasonal.set_title(
            f"Seasonal Components ({total_seasonal_var:.1f}% variance)",
            fontweight="bold",
        )
        ax_seasonal.legend(loc="upper right")
        utils.add_grid(ax_seasonal)
        utils.format_time_axis(ax_seasonal)

        ax_residual.plot(
            timestamps,
            self.decomposition_data["residual"],
            color=config.DECOMPOSITION_COLORS["residual"],
            linewidth=config.LINE_SETTINGS["linewidth_thin"],
            alpha=0.7,
        )
        ax_residual.set_ylabel("Residual")
        ax_residual.set_xlabel("Time")
        resid_var = self._statistics["variance_explained"]["residual"]
        ax_residual.set_title(
            f"Residual ({resid_var:.1f}% variance)", fontweight="bold"
        )
        utils.add_grid(ax_residual)
        utils.format_time_axis(ax_residual)

        if ax_stats is not None:
            ax_stats.axis("off")

            table_data = []

            table_data.append(["Component", "Variance %", "Strength"])

            table_data.append(
                [
                    "Trend",
                    f"{self._statistics['variance_explained']['trend']:.1f}%",
                    "-",
                ]
            )

            for col in self._seasonal_columns:
                period = _extract_period_from_column(col)
                label = (
                    _get_period_label(period, self.period_labels)
                    if period
                    else "Seasonal"
                )
                var_pct = self._statistics["variance_explained"].get(col, 0)
                strength = self._statistics["seasonality_strength"].get(col, 0)
                table_data.append([label, f"{var_pct:.1f}%", f"{strength:.3f}"])

            table_data.append(
                [
                    "Residual",
                    f"{self._statistics['variance_explained']['residual']:.1f}%",
                    "-",
                ]
            )

            table_data.append(["", "", ""])
            table_data.append(["Residual Diagnostics", "", ""])

            diag = self._statistics["residual_diagnostics"]
            table_data.append(["Mean", f"{diag['mean']:.4f}", ""])
            table_data.append(["Std Dev", f"{diag['std']:.4f}", ""])
            table_data.append(["Skewness", f"{diag['skewness']:.3f}", ""])
            table_data.append(["Kurtosis", f"{diag['kurtosis']:.3f}", ""])

            table = ax_stats.table(
                cellText=table_data,
                cellLoc="center",
                loc="center",
                bbox=[0.05, 0.1, 0.9, 0.85],
            )

            table.auto_set_font_size(False)
            table.set_fontsize(config.FONT_SIZES["legend"])
            table.scale(1, 1.5)

            for i in range(len(table_data[0])):
                table[(0, i)].set_facecolor("#2C3E50")
                table[(0, i)].set_text_props(weight="bold", color="white")

            for i in [5, 6]:
                if i < len(table_data):
                    for j in range(len(table_data[0])):
                        table[(i, j)].set_facecolor("#f0f0f0")

            ax_stats.set_title("Decomposition Statistics", fontweight="bold")

        plot_title = self.title
        if plot_title is None:
            if self.sensor_id:
                plot_title = f"Decomposition Dashboard - {self.sensor_id}"
            else:
                plot_title = "Decomposition Dashboard"

        self._fig.suptitle(
            plot_title,
            fontsize=config.FONT_SIZES["title"] + 2,
            fontweight="bold",
            y=0.98,
        )

        self._fig.subplots_adjust(top=0.93, hspace=0.3, left=0.1, right=0.95)

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """
        Save the dashboard to file.

        Args:
            filepath (Union[str, Path]): Output file path.
            dpi (Optional[int]): DPI for output image.
            **kwargs (Any): Additional save options.

        Returns:
            Path: Path to the saved file.
        """
        if self._fig is None:
            self.plot()

        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )


class MultiSensorDecompositionPlot(MatplotlibVisualizationInterface):
    """
    Create decomposition grid for multiple sensors.

    Displays decomposition results for multiple sensors in a grid layout,
    with each cell showing either a compact overlay or expanded view.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.decomposition import MultiSensorDecompositionPlot

    decomposition_dict = {
        "SENSOR_001": df_sensor1,
        "SENSOR_002": df_sensor2,
        "SENSOR_003": df_sensor3,
    }

    plot = MultiSensorDecompositionPlot(
        decomposition_dict=decomposition_dict,
        max_sensors=9,
        period_labels={144: "Day", 1008: "Week"}  # Custom period names
    )
    fig = plot.plot()
    plot.save("multi_sensor_decomposition.png")
    ```

    Parameters:
        decomposition_dict: Dictionary mapping sensor_id to decomposition DataFrame.
        timestamp_column: Name of timestamp column (default: "timestamp")
        value_column: Name of original value column (default: "value")
        max_sensors: Maximum number of sensors to display (default: 9).
        compact: If True, show overlay of components; if False, show stacked (default: True).
        title: Optional main title.
        column_mapping: Optional column name mapping.
        period_labels: Optional mapping from period values to custom display names.
            Example: {144: "Day", 1008: "Week"} maps period 144 to "Day".
    """

    decomposition_dict: Dict[str, PandasDataFrame]
    timestamp_column: str
    value_column: str
    max_sensors: int
    compact: bool
    title: Optional[str]
    column_mapping: Optional[Dict[str, str]]
    period_labels: Optional[Dict[int, str]]
    _fig: Optional[plt.Figure]

    def __init__(
        self,
        decomposition_dict: Dict[str, PandasDataFrame],
        timestamp_column: str = "timestamp",
        value_column: str = "value",
        max_sensors: int = 9,
        compact: bool = True,
        title: Optional[str] = None,
        column_mapping: Optional[Dict[str, str]] = None,
        period_labels: Optional[Dict[int, str]] = None,
    ) -> None:
        self.decomposition_dict = decomposition_dict
        self.timestamp_column = timestamp_column
        self.value_column = value_column
        self.max_sensors = max_sensors
        self.compact = compact
        self.title = title
        self.column_mapping = column_mapping
        self.period_labels = period_labels
        self._fig = None

        if not decomposition_dict:
            raise VisualizationDataError(
                "decomposition_dict cannot be empty. "
                "Please provide at least one sensor's decomposition data."
            )

        for sensor_id, df in decomposition_dict.items():
            df_mapped = apply_column_mapping(df, column_mapping, inplace=False)

            required_cols = [timestamp_column, value_column, "trend", "residual"]
            validate_dataframe(
                df_mapped,
                required_columns=required_cols,
                df_name=f"decomposition_dict['{sensor_id}']",
            )

            seasonal_cols = _get_seasonal_columns(df_mapped)
            if not seasonal_cols:
                raise VisualizationDataError(
                    f"decomposition_dict['{sensor_id}'] must contain at least one "
                    "seasonal column."
                )

    def plot(self) -> plt.Figure:
        """
        Generate the multi-sensor decomposition grid.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        utils.setup_plot_style()

        sensors = list(self.decomposition_dict.keys())[: self.max_sensors]
        n_sensors = len(sensors)

        n_rows, n_cols = config.get_grid_layout(n_sensors)
        figsize = config.get_figsize_for_grid(n_sensors)

        self._fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
        if n_sensors == 1:
            axes = np.array([axes])
        axes = np.array(axes).flatten()

        for idx, sensor_id in enumerate(sensors):
            ax = axes[idx]

            df = apply_column_mapping(
                self.decomposition_dict[sensor_id],
                self.column_mapping,
                inplace=False,
            )

            df = coerce_types(
                df,
                datetime_cols=[self.timestamp_column],
                numeric_cols=[self.value_column, "trend", "residual"],
                inplace=True,
            )

            df = df.sort_values(self.timestamp_column).reset_index(drop=True)

            timestamps = df[self.timestamp_column]
            seasonal_cols = _get_seasonal_columns(df)

            if self.compact:
                ax.plot(
                    timestamps,
                    df[self.value_column],
                    color=config.DECOMPOSITION_COLORS["original"],
                    linewidth=1.5,
                    label="Original",
                    alpha=0.5,
                )

                ax.plot(
                    timestamps,
                    df["trend"],
                    color=config.DECOMPOSITION_COLORS["trend"],
                    linewidth=2,
                    label="Trend",
                )

                for s_idx, col in enumerate(seasonal_cols):
                    period = _extract_period_from_column(col)
                    color = (
                        config.get_seasonal_color(period, s_idx)
                        if period
                        else config.DECOMPOSITION_COLORS["seasonal"]
                    )
                    label = _get_period_label(period, self.period_labels)

                    trend_plus_seasonal = df["trend"] + df[col]
                    ax.plot(
                        timestamps,
                        trend_plus_seasonal,
                        color=color,
                        linewidth=1.5,
                        label=f"Trend + {label}",
                        linestyle="--",
                    )

            else:
                ax.plot(
                    timestamps,
                    df[self.value_column],
                    color=config.DECOMPOSITION_COLORS["original"],
                    linewidth=1.5,
                    label="Original",
                )

            sensor_display = (
                sensor_id[:30] + "..." if len(sensor_id) > 30 else sensor_id
            )
            ax.set_title(sensor_display, fontsize=config.FONT_SIZES["subtitle"])

            if idx == 0:
                ax.legend(loc="upper right", fontsize=config.FONT_SIZES["annotation"])

            utils.add_grid(ax)
            utils.format_time_axis(ax)

        utils.hide_unused_subplots(axes, n_sensors)

        plot_title = self.title
        if plot_title is None:
            plot_title = f"Multi-Sensor Decomposition ({n_sensors} sensors)"

        self._fig.suptitle(
            plot_title,
            fontsize=config.FONT_SIZES["title"] + 2,
            fontweight="bold",
            y=0.98,
        )

        self._fig.subplots_adjust(top=0.93, hspace=0.3, left=0.1, right=0.95)

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: Optional[int] = None,
        **kwargs,
    ) -> Path:
        """
        Save the visualization to file.

        Args:
            filepath (Union[str, Path]): Output file path.
            dpi (Optional[int]): DPI for output image.
            **kwargs (Any): Additional save options.

        Returns:
            Path: Path to the saved file.
        """
        if self._fig is None:
            self.plot()

        return utils.save_plot(
            self._fig,
            str(filepath),
            dpi=dpi,
            close=kwargs.get("close", False),
            verbose=kwargs.get("verbose", True),
        )
