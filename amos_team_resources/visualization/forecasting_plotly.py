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
Plotly-based forecasting visualizations for RTDIP time series.

This module provides interactive Plotly versions of forecasting visualization functions.
All functions mirror the Matplotlib API but return Plotly Figure objects for interactive exploration.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from pathlib import Path
from typing import Optional, Union, Dict, Tuple, List

from . import config


def plot_forecast_with_confidence(
    historical_data: pd.DataFrame,
    forecast_data: pd.DataFrame,
    forecast_start: pd.Timestamp,
    sensor_id: Optional[str] = None,
    ci_levels: List[int] = [60, 80],
    title: Optional[str] = None
) -> go.Figure:
    """
    Create interactive Plotly forecast plot with confidence intervals.

    Args:
        historical_data: Historical time series data
        forecast_data: Forecast predictions with quantiles
        forecast_start: Timestamp where forecast begins
        sensor_id: Sensor identifier for the plot title
        ci_levels: Confidence interval levels to display (default: [60, 80])
        title: Custom plot title

    Returns:
        plotly.graph_objects.Figure: Interactive Plotly figure
    """
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=historical_data['timestamp'],
        y=historical_data['value'],
        mode='lines',
        name='Historical',
        line=dict(color=config.COLORS['historical'], width=1.5),
        hovertemplate='<b>Historical</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=forecast_data['timestamp'],
        y=forecast_data['mean'],
        mode='lines',
        name='Forecast',
        line=dict(color=config.COLORS['forecast'], width=2),
        hovertemplate='<b>Forecast</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>'
    ))

    for ci_level in sorted(ci_levels, reverse=True):
        lower_q = (100 - ci_level) / 200  # e.g., 60% CI -> 0.2
        upper_q = 1 - lower_q              # e.g., 60% CI -> 0.8

        lower_col = f'{lower_q:.1f}'
        upper_col = f'{upper_q:.1f}'

        if lower_col in forecast_data.columns and upper_col in forecast_data.columns:
            fig.add_trace(go.Scatter(
                x=forecast_data['timestamp'],
                y=forecast_data[upper_col],
                mode='lines',
                line=dict(width=0),
                showlegend=False,
                hoverinfo='skip'
            ))

            fig.add_trace(go.Scatter(
                x=forecast_data['timestamp'],
                y=forecast_data[lower_col],
                mode='lines',
                fill='tonexty',
                name=f'{ci_level}% CI',
                fillcolor=config.COLORS['ci_60'] if ci_level == 60 else config.COLORS['ci_80'],
                opacity=0.3 if ci_level == 60 else 0.2,
                line=dict(width=0),
                hovertemplate=f'<b>{ci_level}% CI</b><br>Time: %{{x}}<br>Lower: %{{y:.2f}}<extra></extra>'
            ))

    fig.add_shape(
        type="line",
        x0=forecast_start,
        x1=forecast_start,
        y0=0,
        y1=1,
        yref="paper",
        line=dict(
            color=config.COLORS['forecast_start'],
            width=2,
            dash="dash"
        )
    )

    fig.add_annotation(
        x=forecast_start,
        y=1,
        yref="paper",
        text="Forecast Start",
        showarrow=False,
        yshift=10
    )

    plot_title = title or f'Forecast with Confidence Intervals'
    if sensor_id:
        plot_title += f' - {sensor_id}'

    fig.update_layout(
        title=plot_title,
        xaxis_title='Time',
        yaxis_title='Value',
        hovermode='x unified',
        template='plotly_white',
        height=600,
        showlegend=True,
        legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
    )

    return fig


def plot_forecast_with_actual(
    historical_data: pd.DataFrame,
    forecast_data: pd.DataFrame,
    actual_data: pd.DataFrame,
    forecast_start: pd.Timestamp,
    sensor_id: Optional[str] = None,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create interactive Plotly plot comparing forecast against actual values.

    Args:
        historical_data: Historical time series data
        forecast_data: Forecast predictions
        actual_data: Actual values during forecast period
        forecast_start: Timestamp where forecast begins
        sensor_id: Sensor identifier for the plot title
        title: Custom plot title

    Returns:
        plotly.graph_objects.Figure: Interactive Plotly figure
    """
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=historical_data['timestamp'],
        y=historical_data['value'],
        mode='lines',
        name='Historical',
        line=dict(color=config.COLORS['historical'], width=1.5),
        hovertemplate='<b>Historical</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=forecast_data['timestamp'],
        y=forecast_data['mean'],
        mode='lines',
        name='Forecast',
        line=dict(color=config.COLORS['forecast'], width=2),
        hovertemplate='<b>Forecast</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>'
    ))

    fig.add_trace(go.Scatter(
        x=actual_data['timestamp'],
        y=actual_data['value'],
        mode='lines+markers',
        name='Actual',
        line=dict(color=config.COLORS['actual'], width=2),
        marker=dict(size=4),
        hovertemplate='<b>Actual</b><br>Time: %{x}<br>Value: %{y:.2f}<extra></extra>'
    ))


    fig.add_shape(
        type="line",
        x0=forecast_start,
        x1=forecast_start,
        y0=0,
        y1=1,
        yref="paper",
        line=dict(
            color=config.COLORS['forecast_start'],
            width=2,
            dash="dash"
        )
    )

    fig.add_annotation(
        x=forecast_start,
        y=1,
        yref="paper",
        text="Forecast Start",
        showarrow=False,
        yshift=10
    )

    plot_title = title or 'Forecast vs Actual Values'
    if sensor_id:
        plot_title += f' - {sensor_id}'

    fig.update_layout(
        title=plot_title,
        xaxis_title='Time',
        yaxis_title='Value',
        hovermode='x unified',
        template='plotly_white',
        height=600,
        showlegend=True,
        legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)')
    )

    return fig


def plot_residuals_over_time(
    actual: pd.Series,
    predicted: pd.Series,
    timestamps: pd.Series,
    sensor_id: Optional[str] = None,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create interactive Plotly residuals plot over time.

    Args:
        actual: Actual values
        predicted: Predicted values
        timestamps: Timestamps for x-axis
        sensor_id: Sensor identifier for the plot title
        title: Custom plot title

    Returns:
        plotly.graph_objects.Figure: Interactive Plotly figure
    """
    residuals = actual - predicted

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=timestamps,
        y=residuals,
        mode='lines+markers',
        name='Residuals',
        line=dict(color=config.COLORS['anomaly'], width=1.5),
        marker=dict(size=4),
        hovertemplate='<b>Residual</b><br>Time: %{x}<br>Error: %{y:.2f}<extra></extra>'
    ))

    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="gray",
        annotation_text="Zero Error"
    )

    plot_title = title or 'Residuals Over Time'
    if sensor_id:
        plot_title += f' - {sensor_id}'

    fig.update_layout(
        title=plot_title,
        xaxis_title='Time',
        yaxis_title='Residual (Actual - Predicted)',
        hovermode='x unified',
        template='plotly_white',
        height=500
    )

    return fig


def plot_error_distribution(
    actual: pd.Series,
    predicted: pd.Series,
    sensor_id: Optional[str] = None,
    title: Optional[str] = None,
    bins: int = 30
) -> go.Figure:
    """
    Create interactive Plotly histogram of forecast errors.

    Args:
        actual: Actual values
        predicted: Predicted values
        sensor_id: Sensor identifier for the plot title
        title: Custom plot title
        bins: Number of histogram bins

    Returns:
        plotly.graph_objects.Figure: Interactive Plotly figure
    """
    errors = actual - predicted

    fig = go.Figure()

    fig.add_trace(go.Histogram(
        x=errors,
        nbinsx=bins,
        name='Error Distribution',
        marker_color=config.COLORS['anomaly'],
        opacity=0.7,
        hovertemplate='Error: %{x:.2f}<br>Count: %{y}<extra></extra>'
    ))

    mean_error = errors.mean()
    fig.add_vline(
        x=mean_error,
        line_dash="dash",
        line_color="black",
        annotation_text=f"Mean: {mean_error:.2f}"
    )

    plot_title = title or 'Forecast Error Distribution'
    if sensor_id:
        plot_title += f' - {sensor_id}'

    mae = np.abs(errors).mean()
    rmse = np.sqrt((errors ** 2).mean())

    fig.update_layout(
        title=plot_title,
        xaxis_title='Error (Actual - Predicted)',
        yaxis_title='Frequency',
        template='plotly_white',
        height=500,
        annotations=[
            dict(
                x=0.98,
                y=0.98,
                xref='paper',
                yref='paper',
                text=f'MAE: {mae:.2f}<br>RMSE: {rmse:.2f}',
                showarrow=False,
                bgcolor='rgba(255,255,255,0.8)',
                bordercolor='black',
                borderwidth=1
            )
        ]
    )

    return fig


def plot_scatter_actual_vs_predicted(
    actual: pd.Series,
    predicted: pd.Series,
    sensor_id: Optional[str] = None,
    title: Optional[str] = None
) -> go.Figure:
    """
    Create interactive Plotly scatter plot of actual vs predicted values.

    Args:
        actual: Actual values
        predicted: Predicted values
        sensor_id: Sensor identifier for the plot title
        title: Custom plot title

    Returns:
        plotly.graph_objects.Figure: Interactive Plotly figure
    """
    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=actual,
        y=predicted,
        mode='markers',
        name='Predictions',
        marker=dict(
            color=config.COLORS['forecast'],
            size=8,
            opacity=0.6
        ),
        hovertemplate='<b>Point</b><br>Actual: %{x:.2f}<br>Predicted: %{y:.2f}<extra></extra>'
    ))

    #  Perfect prediction line (y=x)
    min_val = min(actual.min(), predicted.min())
    max_val = max(actual.max(), predicted.max())

    fig.add_trace(go.Scatter(
        x=[min_val, max_val],
        y=[min_val, max_val],
        mode='lines',
        name='Perfect Prediction',
        line=dict(color='gray', dash='dash', width=2),
        hoverinfo='skip'
    ))

    from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

    mae = mean_absolute_error(actual, predicted)
    rmse = np.sqrt(mean_squared_error(actual, predicted))
    r2 = r2_score(actual, predicted)

    # Update layout
    plot_title = title or 'Actual vs Predicted Values'
    if sensor_id:
        plot_title += f' - {sensor_id}'

    fig.update_layout(
        title=plot_title,
        xaxis_title='Actual Value',
        yaxis_title='Predicted Value',
        template='plotly_white',
        height=600,
        annotations=[
            dict(
                x=0.98,
                y=0.02,
                xref='paper',
                yref='paper',
                text=f'R²: {r2:.4f}<br>MAE: {mae:.2f}<br>RMSE: {rmse:.2f}',
                showarrow=False,
                bgcolor='rgba(255,255,255,0.8)',
                bordercolor='black',
                borderwidth=1,
                align='left'
            )
        ]
    )

    fig.update_xaxes(scaleanchor="y", scaleratio=1)

    return fig


def save_plotly_figure(fig: go.Figure, filename: Union[str, Path], format: str = 'html'):
    """
    Save Plotly figure to file.

    Args:
        fig: Plotly figure object
        filename: Output filename
        format: Output format - 'html' (interactive) or 'png' (static, requires kaleido)
    """
    filename = Path(filename)
    filename.parent.mkdir(parents=True, exist_ok=True)

    if format == 'html':
        if not str(filename).endswith('.html'):
            filename = filename.with_suffix('.html')
        fig.write_html(filename)
    elif format == 'png':
        if not str(filename).endswith('.png'):
            filename = filename.with_suffix('.png')
        fig.write_image(filename, width=1200, height=800, scale=2)
    else:
        raise ValueError(f"Unsupported format: {format}. Use 'html' or 'png'.")

    print(f"Saved: {filename}")
