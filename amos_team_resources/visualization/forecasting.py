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
Time series forecasting visualization module.

This module provides standardized visualization functions for forecasting results,
including confidence intervals, model comparisons, and error analysis.

Functions answer the following questions:
- How accurate are the predictions? (plot_forecast_with_actual)
- What is prediction uncertainty? (plot_forecast_with_confidence)
- What is the error distribution? (plot_error_distribution)
- Are errors systematic? (plot_residuals_over_time)
- How do predictions compare to actual? (plot_scatter_actual_vs_predicted)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Optional, Union, Dict, Tuple, List
import warnings

from . import config
from . import utils

warnings.filterwarnings('ignore')


# SINGLE SENSOR FORECASTING VISUALIZATIONS

def plot_forecast_with_confidence(
    historical_data: pd.DataFrame,
    forecast_data: pd.DataFrame,
    forecast_start: pd.Timestamp,
    sensor_id: Optional[str] = None,
    lookback_hours: int = 168,
    ci_levels: List[int] = [60, 80],
    ax: Optional[plt.Axes] = None,
    title: Optional[str] = None,
    show_legend: bool = True
) -> plt.Axes:
    """
    Plot time series forecast with confidence intervals.

    Args:
        historical_data: DataFrame with columns ['timestamp', 'value']
        forecast_data: DataFrame with columns ['timestamp', 'mean', '0.1', '0.2', '0.8', '0.9']
        forecast_start: Timestamp marking start of forecast
        sensor_id: Sensor identifier for title
        lookback_hours: Hours of historical data to show (default: 168 = 1 week)
        ci_levels: List of confidence interval levels to plot (default: [60, 80])
        ax: Matplotlib axis (if None, creates new figure)
        title: Custom title (if None, auto-generated)
        show_legend: Whether to show legend

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    ax.plot(
        historical_data['timestamp'],
        historical_data['value'],
        'o-',
        color=config.COLORS['historical'],
        label='Historical Data',
        linewidth=config.LINE_SETTINGS['linewidth'],
        markersize=config.LINE_SETTINGS['marker_size'],
        alpha=0.8
    )

    ax.plot(
        forecast_data['timestamp'],
        forecast_data['mean'],
        's-',
        color=config.COLORS['forecast'],
        label='Forecast (mean)',
        linewidth=config.LINE_SETTINGS['linewidth'],
        markersize=config.LINE_SETTINGS['marker_size'],
        alpha=0.9
    )

    for ci_level in sorted(ci_levels, reverse=True):  
        if ci_level == 60 and '0.2' in forecast_data.columns and '0.8' in forecast_data.columns:
            utils.plot_confidence_intervals(
                ax,
                forecast_data['timestamp'],
                forecast_data['0.2'],
                forecast_data['0.8'],
                ci_level=60
            )
        elif ci_level == 80 and '0.1' in forecast_data.columns and '0.9' in forecast_data.columns:
            utils.plot_confidence_intervals(
                ax,
                forecast_data['timestamp'],
                forecast_data['0.1'],
                forecast_data['0.9'],
                ci_level=80
            )
        elif ci_level == 90 and '0.05' in forecast_data.columns and '0.95' in forecast_data.columns:
            utils.plot_confidence_intervals(
                ax,
                forecast_data['timestamp'],
                forecast_data['0.05'],
                forecast_data['0.95'],
                ci_level=90
            )

    utils.add_vertical_line(ax, forecast_start, label='Forecast Start')

    if title is None and sensor_id:
        title = f'{sensor_id} - Forecast with Confidence Intervals'
    elif title is None:
        title = 'Time Series Forecast with Confidence Intervals'

    utils.format_axis(
        ax,
        title=title,
        xlabel='Time',
        ylabel='Value',
        add_legend=show_legend,
        grid=True,
        time_axis=True
    )

    return ax


def plot_forecast_with_actual(
    historical_data: pd.DataFrame,
    forecast_data: pd.DataFrame,
    actual_data: pd.DataFrame,
    forecast_start: pd.Timestamp,
    sensor_id: Optional[str] = None,
    lookback_hours: int = 168,
    ax: Optional[plt.Axes] = None
) -> plt.Axes:
    """
    Plot forecast against actual values (ground truth comparison).

    Args:
        historical_data: DataFrame with columns ['timestamp', 'value']
        forecast_data: DataFrame with columns ['timestamp', 'mean']
        actual_data: DataFrame with actual values during forecast period
        forecast_start: Timestamp marking start of forecast
        sensor_id: Sensor identifier
        lookback_hours: Hours of historical data to show
        ax: Matplotlib axis (if None, creates new figure)

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    ax.plot(
        historical_data['timestamp'],
        historical_data['value'],
        'o-',
        color=config.COLORS['historical'],
        label='Historical',
        linewidth=config.LINE_SETTINGS['linewidth'],
        markersize=config.LINE_SETTINGS['marker_size'],
        alpha=0.7
    )

    ax.plot(
        actual_data['timestamp'],
        actual_data['value'],
        'o-',
        color=config.COLORS['actual'],
        label='Actual',
        linewidth=config.LINE_SETTINGS['linewidth'],
        markersize=config.LINE_SETTINGS['marker_size'],
        alpha=0.8
    )

    ax.plot(
        forecast_data['timestamp'],
        forecast_data['mean'],
        's-',
        color=config.COLORS['forecast'],
        label='Forecast',
        linewidth=config.LINE_SETTINGS['linewidth'],
        markersize=config.LINE_SETTINGS['marker_size'],
        alpha=0.9
    )

    utils.add_vertical_line(ax, forecast_start, label='Forecast Start')

    title = f'{sensor_id} - Forecast vs Actual' if sensor_id else 'Forecast vs Actual Values'
    utils.format_axis(
        ax,
        title=title,
        xlabel='Time',
        ylabel='Value',
        add_legend=True,
        grid=True,
        time_axis=True
    )

    return ax


# MULTI-SENSOR OVERVIEW PLOTS

def plot_multi_sensor_overview(
    predictions_df: pd.DataFrame,
    historical_df: pd.DataFrame,
    lookback_hours: int = 168,
    max_sensors: Optional[int] = None,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Figure:
    """
    Create multi-sensor overview plot in grid layout.

    Args:
        predictions_df: DataFrame with columns ['item_id', 'timestamp', 'mean', ...]
        historical_df: DataFrame with columns ['TagName', 'EventTime', 'Value']
        lookback_hours: Hours of historical data to show
        max_sensors: Maximum number of sensors to plot (default: all)
        output_path: Path to save figure (if None, doesn't save)

    Returns:
        plt.Figure: Matplotlib figure object
    """
    utils.setup_plot_style()

    sensors = predictions_df['item_id'].unique()
    if max_sensors:
        sensors = sensors[:max_sensors]

    n_sensors = len(sensors)
    n_rows, n_cols = config.get_grid_layout(n_sensors)
    figsize = config.get_figsize_for_grid(n_sensors)

    fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
    if n_sensors == 1:
        axes = np.array([axes])
    axes = axes.flatten()

    for idx, sensor in enumerate(sensors):
        ax = axes[idx]

        sensor_preds = predictions_df[predictions_df['item_id'] == sensor].sort_values('timestamp')
        if len(sensor_preds) == 0:
            ax.text(0.5, 0.5, f'No data for {sensor}',
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(sensor[:40], fontsize=config.FONT_SIZES['subtitle'])
            continue

        forecast_start = sensor_preds['timestamp'].min()

        sensor_hist = historical_df[historical_df['TagName'] == sensor].copy()
        sensor_hist = sensor_hist.sort_values('EventTime')
        cutoff_time = forecast_start - pd.Timedelta(hours=lookback_hours)
        sensor_hist = sensor_hist[
            (sensor_hist['EventTime'] >= cutoff_time) &
            (sensor_hist['EventTime'] < forecast_start)
        ]

        historical_data = pd.DataFrame({
            'timestamp': sensor_hist['EventTime'],
            'value': sensor_hist['Value']
        })

        plot_forecast_with_confidence(
            historical_data,
            sensor_preds,
            forecast_start,
            sensor_id=sensor[:40],
            lookback_hours=lookback_hours,
            ax=ax,
            show_legend=(idx == 0)  
        )

    utils.hide_unused_subplots(axes, n_sensors)

    plt.suptitle('Forecasts - All Sensors',
                 fontsize=config.FONT_SIZES['title'] + 2,
                 fontweight='bold',
                 y=1.0)
    plt.tight_layout()

    if output_path:
        utils.save_plot(fig, output_path, close=False)

    return fig


# ERROR ANALYSIS VISUALIZATIONS

def plot_residuals_over_time(
    actual: pd.Series,
    predicted: pd.Series,
    timestamps: pd.Series,
    sensor_id: Optional[str] = None,
    ax: Optional[plt.Axes] = None
) -> plt.Axes:
    """
    Plot residuals (actual - predicted) over time.

    Args:
        actual: Actual values
        predicted: Predicted values
        timestamps: Timestamps
        sensor_id: Sensor identifier
        ax: Matplotlib axis (if None, creates new figure)

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    residuals = actual - predicted

    ax.plot(
        timestamps,
        residuals,
        'o-',
        color=config.COLORS['actual'],
        linewidth=config.LINE_SETTINGS['linewidth_thin'],
        markersize=config.LINE_SETTINGS['marker_size'],
        alpha=0.7
    )

    ax.axhline(0, color='black', linestyle='--', linewidth=1.5, alpha=0.5, label='Zero Error')

    mean_residual = residuals.mean()
    ax.axhline(
        mean_residual,
        color=config.COLORS['anomaly'],
        linestyle=':',
        linewidth=1.5,
        alpha=0.7,
        label=f'Mean Residual: {mean_residual:.3f}'
    )

    title = f'{sensor_id} - Residuals Over Time' if sensor_id else 'Residuals Over Time'
    utils.format_axis(
        ax,
        title=title,
        xlabel='Time',
        ylabel='Residual (Actual - Predicted)',
        add_legend=True,
        grid=True,
        time_axis=True
    )

    return ax


def plot_error_distribution(
    actual: pd.Series,
    predicted: pd.Series,
    sensor_id: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    bins: int = 30
) -> plt.Axes:
    """
    Plot histogram of forecast errors.

    Args:
        actual: Actual values
        predicted: Predicted values
        sensor_id: Sensor identifier
        ax: Matplotlib axis (if None, creates new figure)
        bins: Number of histogram bins

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    errors = actual - predicted

    ax.hist(
        errors,
        bins=bins,
        color=config.COLORS['actual'],
        alpha=0.7,
        edgecolor='black',
        linewidth=0.5
    )

    mean_error = errors.mean()
    median_error = errors.median()

    ax.axvline(mean_error, color='red', linestyle='--', linewidth=2,
               label=f'Mean: {mean_error:.3f}')
    ax.axvline(median_error, color='orange', linestyle='--', linewidth=2,
               label=f'Median: {median_error:.3f}')
    ax.axvline(0, color='black', linestyle='-', linewidth=1.5, alpha=0.5)

    std_error = errors.std()
    stats_text = f'Std: {std_error:.3f}\nMAE: {np.abs(errors).mean():.3f}'
    ax.text(
        0.98, 0.98,
        stats_text,
        transform=ax.transAxes,
        verticalalignment='top',
        horizontalalignment='right',
        bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
        fontsize=config.FONT_SIZES['annotation']
    )

    title = f'{sensor_id} - Error Distribution' if sensor_id else 'Forecast Error Distribution'
    utils.format_axis(
        ax,
        title=title,
        xlabel='Error (Actual - Predicted)',
        ylabel='Frequency',
        add_legend=True,
        grid=True
    )

    return ax


def plot_scatter_actual_vs_predicted(
    actual: pd.Series,
    predicted: pd.Series,
    sensor_id: Optional[str] = None,
    ax: Optional[plt.Axes] = None,
    show_metrics: bool = True
) -> plt.Axes:
    """
    Scatter plot of actual vs predicted values.
    
    Args:
        actual: Actual values
        predicted: Predicted values
        sensor_id: Sensor identifier
        ax: Matplotlib axis (if None, creates new figure)
        show_metrics: Whether to show metrics (R², RMSE, etc.)

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    ax.scatter(
        actual,
        predicted,
        alpha=0.6,
        s=config.LINE_SETTINGS['scatter_size'],
        color=config.COLORS['actual'],
        edgecolors='black',
        linewidth=0.5
    )

    min_val = min(actual.min(), predicted.min())
    max_val = max(actual.max(), predicted.max())
    ax.plot(
        [min_val, max_val],
        [min_val, max_val],
        'r--',
        linewidth=2,
        label='Perfect Prediction',
        alpha=0.7
    )

    if show_metrics:
        from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

        r2 = r2_score(actual, predicted)
        rmse = np.sqrt(mean_squared_error(actual, predicted))
        mae = mean_absolute_error(actual, predicted)

        metrics_text = f'R² = {r2:.4f}\nRMSE = {rmse:.3f}\nMAE = {mae:.3f}'
        ax.text(
            0.05, 0.95,
            metrics_text,
            transform=ax.transAxes,
            verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8),
            fontsize=config.FONT_SIZES['annotation']
        )

    title = f'{sensor_id} - Actual vs Predicted' if sensor_id else 'Actual vs Predicted Values'
    utils.format_axis(
        ax,
        title=title,
        xlabel='Actual Value',
        ylabel='Predicted Value',
        add_legend=True,
        grid=True
    )

    ax.set_aspect('equal', adjustable='datalim')

    return ax


# DASHBOARD

def create_forecast_dashboard(
    historical_data: pd.DataFrame,
    forecast_data: pd.DataFrame,
    actual_data: pd.DataFrame,
    forecast_start: pd.Timestamp,
    sensor_id: Optional[str] = None,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Figure:
    """
    Create forecast dashboard with multiple views.

    Includes:
    1. Forecast with confidence intervals
    2. Forecast vs actual
    3. Residuals over time
    4. Error distribution
    5. Actual vs predicted scatter
    6. Metrics table

    Args:
        historical_data: Historical time series data
        forecast_data: Forecast predictions with confidence intervals
        actual_data: Actual values during forecast period
        forecast_start: Start of forecast period
        sensor_id: Sensor identifier
        output_path: Path to save figure

    Returns:
        plt.Figure: Matplotlib figure with 6 subplots
    """
    utils.setup_plot_style()

    fig = plt.figure(figsize=config.FIGSIZE['dashboard'])
    gs = fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

    ax1 = fig.add_subplot(gs[0, 0])
    plot_forecast_with_confidence(
        historical_data,
        forecast_data,
        forecast_start,
        sensor_id=sensor_id,
        ax=ax1
    )

    ax2 = fig.add_subplot(gs[0, 1])
    plot_forecast_with_actual(
        historical_data,
        forecast_data,
        actual_data,
        forecast_start,
        sensor_id=sensor_id,
        ax=ax2
    )

    merged = pd.merge(
        forecast_data[['timestamp', 'mean']],
        actual_data[['timestamp', 'value']],
        on='timestamp',
        how='inner'
    )

    if len(merged) > 0:
        ax3 = fig.add_subplot(gs[1, 0])
        plot_residuals_over_time(
            merged['value'],
            merged['mean'],
            merged['timestamp'],
            sensor_id=sensor_id,
            ax=ax3
        )

        ax4 = fig.add_subplot(gs[1, 1])
        plot_error_distribution(
            merged['value'],
            merged['mean'],
            sensor_id=sensor_id,
            ax=ax4
        )

        ax5 = fig.add_subplot(gs[2, 0])
        plot_scatter_actual_vs_predicted(
            merged['value'],
            merged['mean'],
            sensor_id=sensor_id,
            ax=ax5
        )
    else:
        ax3 = fig.add_subplot(gs[1, 0])
        ax3.text(0.5, 0.5, 'No overlapping timestamps\nfor error analysis',
                ha='center', va='center', transform=ax3.transAxes,
                fontsize=12, color='red')
        ax3.axis('off')

        ax4 = fig.add_subplot(gs[1, 1])
        ax4.axis('off')

        ax5 = fig.add_subplot(gs[2, 0])
        ax5.axis('off')

    ax6 = fig.add_subplot(gs[2, 1])
    ax6.axis('off')

    if len(merged) > 0:
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        mae = mean_absolute_error(merged['value'], merged['mean'])
        mse = mean_squared_error(merged['value'], merged['mean'])
        rmse = np.sqrt(mse)
        mape = np.mean(np.abs((merged['value'] - merged['mean']) / merged['value'])) * 100
        r2 = r2_score(merged['value'], merged['mean'])

        metrics_data = [
            ['MAE', f'{mae:.4f}'],
            ['MSE', f'{mse:.4f}'],
            ['RMSE', f'{rmse:.4f}'],
            ['MAPE', f'{mape:.2f}%'],
            ['R²', f'{r2:.4f}'],
        ]

        table = ax6.table(
            cellText=metrics_data,
            colLabels=['Metric', 'Value'],
            cellLoc='left',
            loc='center',
            bbox=[0.1, 0.3, 0.8, 0.6]
        )
        table.auto_set_font_size(False)
        table.set_fontsize(config.FONT_SIZES['legend'])
        table.scale(1, 2)

        for i in range(2):
            table[(0, i)].set_facecolor('#2C3E50')
            table[(0, i)].set_text_props(weight='bold', color='white')

        ax6.set_title('Forecast Metrics', fontsize=config.FONT_SIZES['title'], fontweight='bold', pad=20)

    main_title = f'Forecast Dashboard - {sensor_id}' if sensor_id else 'Forecast Dashboard'
    fig.suptitle(main_title, fontsize=config.FONT_SIZES['title'] + 2, fontweight='bold', y=0.98)

    if output_path:
        utils.save_plot(fig, output_path, close=False)

    return fig


# HELPER FUNCTIONS

def prepare_sensor_data(
    sensor_id: str,
    predictions_df: pd.DataFrame,
    historical_df: pd.DataFrame,
    lookback_hours: int = 168
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    """
    Prepare data for a single sensor visualization.

    Args:
        sensor_id: Sensor identifier
        predictions_df: Predictions DataFrame with columns ['item_id', 'timestamp', ...]
        historical_df: Historical DataFrame with columns ['TagName', 'EventTime', 'Value']
        lookback_hours: Hours of historical data to include

    Returns:
        tuple: (historical_data, forecast_data, forecast_start)
    """
    sensor_preds = predictions_df[predictions_df['item_id'] == sensor_id].sort_values('timestamp')

    if len(sensor_preds) == 0:
        return None, None, None

    forecast_start = sensor_preds['timestamp'].min()

    sensor_hist = historical_df[historical_df['TagName'] == sensor_id].copy()
    sensor_hist = sensor_hist.sort_values('EventTime')
    cutoff_time = forecast_start - pd.Timedelta(hours=lookback_hours)
    sensor_hist = sensor_hist[
        (sensor_hist['EventTime'] >= cutoff_time) &
        (sensor_hist['EventTime'] < forecast_start)
    ]

    historical_data = pd.DataFrame({
        'timestamp': sensor_hist['EventTime'],
        'value': sensor_hist['Value']
    })

    forecast_data = sensor_preds.copy()

    return historical_data, forecast_data, forecast_start
