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
Common utility functions for RTDIP time series visualization.

This module provides reusable functions for plot setup, saving, formatting,
and other common visualization tasks.
"""

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional, Tuple, Union
import warnings

from . import config

warnings.filterwarnings('ignore')


# PLOT SETUP AND CONFIGURATION

def setup_plot_style():
    """
    Apply standard plotting style to all matplotlib plots.

    Call this at the beginning of any visualization script to ensure
    consistent styling across all plots.
    """
    plt.style.use(config.STYLE)

    plt.rcParams.update({
        'axes.titlesize': config.FONT_SIZES['title'],
        'axes.labelsize': config.FONT_SIZES['axis_label'],
        'xtick.labelsize': config.FONT_SIZES['tick_label'],
        'ytick.labelsize': config.FONT_SIZES['tick_label'],
        'legend.fontsize': config.FONT_SIZES['legend'],
        'figure.titlesize': config.FONT_SIZES['title'],
    })


def create_figure(figsize: Optional[Tuple[float, float]] = None,
                  n_subplots: int = 1,
                  layout: Optional[str] = None) -> Tuple:
    """
    Create a matplotlib figure with standardized settings.

    Args:
        figsize: Figure size (width, height) in inches. If None, auto-calculated
                 based on n_subplots
        n_subplots: Number of subplots needed (used to auto-calculate figsize)
        layout: Layout type ('grid' or 'vertical'). If None, single plot assumed

    Returns:
        tuple: (fig, axes) - matplotlib figure and axes objects
    """
    if figsize is None:
        figsize = config.get_figsize_for_grid(n_subplots)

    if n_subplots == 1:
        fig, ax = plt.subplots(figsize=figsize)
        return fig, ax
    elif layout == 'grid':
        n_rows, n_cols = config.get_grid_layout(n_subplots)
        fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
        axes = np.array(axes).flatten()
        return fig, axes
    elif layout == 'vertical':
        fig, axes = plt.subplots(n_subplots, 1, figsize=figsize)
        if n_subplots == 1:
            axes = [axes]
        return fig, axes
    else:
        n_rows, n_cols = config.get_grid_layout(n_subplots)
        fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
        axes = np.array(axes).flatten()
        return fig, axes


# PLOT SAVING

def save_plot(fig,
              filename: str,
              output_dir: Optional[Union[str, Path]] = None,
              dpi: Optional[int] = None,
              close: bool = True,
              verbose: bool = True) -> Path:
    """
    Save a matplotlib figure with standardized settings.

    Args:
        fig: Matplotlib figure object
        filename: Output filename (with or without extension)
        output_dir: Output directory path. If None, uses config.DEFAULT_OUTPUT_DIR
        dpi: DPI for output image. If None, uses config.EXPORT['dpi']
        close: Whether to close the figure after saving (default: True)
        verbose: Whether to print save confirmation (default: True)

    Returns:
        Path: Full path to saved file
    """
    filename_path = Path(filename)

    valid_extensions = ('.png', '.jpg', '.jpeg', '.pdf', '.svg')
    has_extension = filename_path.suffix.lower() in valid_extensions

    if filename_path.parent != Path('.'):
        if not has_extension:
            filename_path = filename_path.with_suffix(f'.{config.EXPORT["format"]}')
        output_path = filename_path
        output_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        if output_dir is None:
            output_dir = config.DEFAULT_OUTPUT_DIR

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if not has_extension:
            filename_path = filename_path.with_suffix(f'.{config.EXPORT["format"]}')

        output_path = output_dir / filename_path

    if dpi is None:
        dpi = config.EXPORT['dpi']

    fig.savefig(
        output_path,
        dpi=dpi,
        bbox_inches=config.EXPORT['bbox_inches'],
        facecolor=config.EXPORT['facecolor'],
        edgecolor=config.EXPORT['edgecolor']
    )

    if verbose:
        print(f"Saved: {output_path}")

    if close:
        plt.close(fig)

    return output_path


# AXIS FORMATTING

def format_time_axis(ax,
                     rotation: int = 45,
                     time_format: Optional[str] = None):
    """
    Format time-based x-axis with standard settings.

    Args:
        ax: Matplotlib axis object
        rotation: Rotation angle for tick labels (default: 45)
        time_format: strftime format string. If None, uses config default
    """
    ax.tick_params(axis='x', rotation=rotation)

    if time_format:
        import matplotlib.dates as mdates
        ax.xaxis.set_major_formatter(mdates.DateFormatter(time_format))


def add_grid(ax,
             alpha: Optional[float] = None,
             linestyle: Optional[str] = None,
             linewidth: Optional[float] = None):
    """
    Add grid to axis with standard settings.

    Args:
        ax: Matplotlib axis object
        alpha: Grid transparency (default: from config)
        linestyle: Grid line style (default: from config)
        linewidth: Grid line width (default: from config)
    """
    if alpha is None:
        alpha = config.GRID['alpha']
    if linestyle is None:
        linestyle = config.GRID['linestyle']
    if linewidth is None:
        linewidth = config.GRID['linewidth']

    ax.grid(True, alpha=alpha, linestyle=linestyle, linewidth=linewidth)


def format_axis(ax,
                title: Optional[str] = None,
                xlabel: Optional[str] = None,
                ylabel: Optional[str] = None,
                add_legend: bool = True,
                grid: bool = True,
                time_axis: bool = False):
    """
    Apply standard formatting to an axis.

    Args:
        ax: Matplotlib axis object
        title: Plot title
        xlabel: X-axis label
        ylabel: Y-axis label
        add_legend: Whether to add legend (default: True)
        grid: Whether to add grid (default: True)
        time_axis: Whether x-axis is time-based (applies special formatting)
    """
    if title:
        ax.set_title(title, fontsize=config.FONT_SIZES['title'], fontweight='bold')

    if xlabel:
        ax.set_xlabel(xlabel, fontsize=config.FONT_SIZES['axis_label'])

    if ylabel:
        ax.set_ylabel(ylabel, fontsize=config.FONT_SIZES['axis_label'])

    if add_legend:
        ax.legend(loc='best', fontsize=config.FONT_SIZES['legend'])

    if grid:
        add_grid(ax)

    if time_axis:
        format_time_axis(ax)


# DATA PREPARATION

def prepare_time_series_data(df: pd.DataFrame,
                              time_col: str = 'timestamp',
                              value_col: str = 'value',
                              sort: bool = True) -> pd.DataFrame:
    """
    Prepare time series data for plotting.

    Args:
        df: Input dataframe
        time_col: Name of timestamp column
        value_col: Name of value column
        sort: Whether to sort by timestamp

    Returns:
        pd.DataFrame: Prepared dataframe with datetime index
    """
    df = df.copy()

    # Ensure timestamp is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[time_col]):
        df[time_col] = pd.to_datetime(df[time_col])

    # Sort by time if requested
    if sort:
        df = df.sort_values(time_col)

    return df


def convert_spark_to_pandas(spark_df,
                             sort_by: Optional[str] = None) -> pd.DataFrame:
    """
    Convert Spark DataFrame to Pandas DataFrame for plotting.

    Args:
        spark_df: Spark DataFrame
        sort_by: Column to sort by (typically 'timestamp')

    Returns:
        pd.DataFrame: Pandas DataFrame
    """
    pdf = spark_df.toPandas()

    if sort_by:
        pdf = pdf.sort_values(sort_by)

    return pdf


# CONFIDENCE INTERVAL PLOTTING

def plot_confidence_intervals(ax,
                               timestamps,
                               lower_bounds,
                               upper_bounds,
                               ci_level: int = 80,
                               color: Optional[str] = None,
                               label: Optional[str] = None):
    """
    Plot shaded confidence interval region.

    Args:
        ax: Matplotlib axis object
        timestamps: X-axis values (timestamps)
        lower_bounds: Lower bound values
        upper_bounds: Upper bound values
        ci_level: Confidence interval level (60, 80, or 90)
        color: Fill color (default: from config)
        label: Label for legend
    """
    if color is None:
        color = config.COLORS['ci_80']

    alpha = config.CI_ALPHA.get(ci_level, 0.2)

    if label is None:
        label = f'{ci_level}% CI'

    ax.fill_between(
        timestamps,
        lower_bounds,
        upper_bounds,
        color=color,
        alpha=alpha,
        label=label
    )


# METRIC FORMATTING

def format_metric_value(metric_name: str, value: float) -> str:
    """
    Format a metric value according to standard display format.

    Args:
        metric_name: Name of the metric (e.g., 'mae', 'rmse')
        value: Metric value

    Returns:
        str: Formatted string
    """
    metric_name = metric_name.lower()

    if metric_name in config.METRICS:
        fmt = config.METRICS[metric_name]['format']
        display_name = config.METRICS[metric_name]['name']
        return f"{display_name}: {value:{fmt}}"
    else:
        return f"{metric_name}: {value:.3f}"


def create_metrics_table(metrics_dict: dict,
                          model_name: Optional[str] = None) -> pd.DataFrame:
    """
    Create a formatted DataFrame of metrics.

    Args:
        metrics_dict: Dictionary of metric name -> value
        model_name: Optional model name to include in table

    Returns:
        pd.DataFrame: Formatted metrics table
    """
    data = []

    for metric_name, value in metrics_dict.items():
        if metric_name.lower() in config.METRICS:
            display_name = config.METRICS[metric_name.lower()]['name']
        else:
            display_name = metric_name.upper()

        data.append({
            'Metric': display_name,
            'Value': value
        })

    df = pd.DataFrame(data)

    if model_name:
        df.insert(0, 'Model', model_name)

    return df


# ANNOTATION HELPERS

def add_vertical_line(ax,
                      x_position,
                      label: str,
                      color: Optional[str] = None,
                      linestyle: str = '--',
                      linewidth: float = 2.0,
                      alpha: float = 0.7):
    """
    Add a vertical line to mark important positions (e.g., forecast start).

    Args:
        ax: Matplotlib axis object
        x_position: X coordinate for the line
        label: Label for legend
        color: Line color (default: red from config)
        linestyle: Line style (default: '--')
        linewidth: Line width (default: 2.0)
        alpha: Line transparency (default: 0.7)
    """
    if color is None:
        color = config.COLORS['forecast_start']

    ax.axvline(
        x_position,
        color=color,
        linestyle=linestyle,
        linewidth=linewidth,
        alpha=alpha,
        label=label
    )


def add_text_annotation(ax,
                        x, y,
                        text: str,
                        fontsize: Optional[int] = None,
                        color: str = 'black',
                        bbox: bool = True):
    """
    Add text annotation to plot.

    Args:
        ax: Matplotlib axis object
        x: X coordinate (in data coordinates)
        y: Y coordinate (in data coordinates)
        text: Text to display
        fontsize: Font size (default: from config)
        color: Text color
        bbox: Whether to add background box
    """
    if fontsize is None:
        fontsize = config.FONT_SIZES['annotation']

    bbox_props = None
    if bbox:
        bbox_props = dict(boxstyle='round,pad=0.5', facecolor='white', alpha=0.7)

    ax.annotate(
        text,
        xy=(x, y),
        fontsize=fontsize,
        color=color,
        bbox=bbox_props
    )


# SUBPLOT MANAGEMENT

def hide_unused_subplots(axes, n_used: int):
    """
    Hide unused subplots in a grid layout.

    Args:
        axes: Flattened array of matplotlib axes
        n_used: Number of subplots actually used
    """
    axes = np.array(axes).flatten()
    for idx in range(n_used, len(axes)):
        axes[idx].axis('off')


def add_subplot_labels(axes, labels: list):
    """
    Add letter labels (A, B, C, etc.) to subplots.

    Args:
        axes: Array of matplotlib axes
        labels: List of labels (e.g., ['A', 'B', 'C'])
    """
    axes = np.array(axes).flatten()
    for ax, label in zip(axes, labels):
        ax.text(
            -0.1, 1.1, label,
            transform=ax.transAxes,
            fontsize=config.FONT_SIZES['title'],
            fontweight='bold',
            va='top'
        )


# COLOR HELPERS

def get_color_cycle(n_colors: int, colorblind_safe: bool = False) -> list:
    """
    Get a list of colors for multi-line plots.

    Args:
        n_colors: Number of colors needed
        colorblind_safe: Whether to use colorblind-friendly palette

    Returns:
        list: List of color hex codes
    """
    if colorblind_safe or n_colors > len(config.MODEL_COLORS):
        colors = config.COLORBLIND_PALETTE
        return [colors[i % len(colors)] for i in range(n_colors)]
    else:
        import matplotlib.pyplot as plt
        prop_cycle = plt.rcParams['axes.prop_cycle']
        colors = prop_cycle.by_key()['color']
        return [colors[i % len(colors)] for i in range(n_colors)]
