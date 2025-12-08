"""
Model comparison visualization module.

This module provides visualization functions for comparing multiple forecasting models,
including performance metrics, leaderboards, and side-by-side forecast comparisons.

- How do different models compare (plot_model_performance_comparison)
- Which model performs best (plot_model_leaderboard)
- How do model predictions differ (plot_models_side_by_side)
- What are the prediction distributions (plot_forecast_distributions)
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Optional, Union, Dict, List
import warnings

from . import config
from . import utils

warnings.filterwarnings('ignore')


# MODEL PERFORMANCE COMPARISON
def plot_model_performance_comparison(
    metrics_dict: Dict[str, Dict[str, float]],
    metrics_to_plot: Optional[List[str]] = None,
    ax: Optional[plt.Axes] = None,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Axes:
    """
    Create bar chart comparing model performance across metrics.
    
    Args:
        metrics_dict: Dictionary of {model_name: {metric_name: value}}
        metrics_to_plot: List of metrics to include (default: all in config.METRIC_ORDER)
        ax: Matplotlib axis (if None, creates new figure)
        output_path: Path to save figure

    Returns:
        plt.Axes: Matplotlib axis object

    Example:
        metrics_dict = {
            'AutoGluon': {'mae': 1.23, 'rmse': 2.45, 'mape': 10.5},
            'LSTM': {'mae': 1.45, 'rmse': 2.67, 'mape': 12.3},
            'XGBoost': {'mae': 1.34, 'rmse': 2.56, 'mape': 11.2}
        }
        plot_model_performance_comparison(metrics_dict)
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['comparison'])
        
    df = pd.DataFrame(metrics_dict).T
    
    if metrics_to_plot is None:
        metrics_to_plot = [m for m in config.METRIC_ORDER if m in df.columns]
    else:
        metrics_to_plot = [m for m in metrics_to_plot if m in df.columns]

    df = df[metrics_to_plot]

    x = np.arange(len(df.columns))
    width = 0.8 / len(df.index)

    models = df.index.tolist()

    for i, model in enumerate(models):
        color = config.get_model_color(model)
        offset = (i - len(models)/2 + 0.5) * width

        ax.bar(
            x + offset,
            df.loc[model],
            width,
            label=model,
            color=color,
            alpha=0.8,
            edgecolor='black',
            linewidth=0.5
        )

    ax.set_xlabel('Metric', fontweight='bold', fontsize=config.FONT_SIZES['axis_label'])
    ax.set_ylabel('Value (lower is better)', fontweight='bold', fontsize=config.FONT_SIZES['axis_label'])
    ax.set_title('Model Performance Comparison', fontweight='bold', fontsize=config.FONT_SIZES['title'])
    ax.set_xticks(x)
    ax.set_xticklabels([config.METRICS.get(m, {'name': m.upper()})['name'] for m in df.columns])
    ax.legend(fontsize=config.FONT_SIZES['legend'])
    utils.add_grid(ax)

    plt.tight_layout()

    if output_path:
        utils.save_plot(ax.figure, output_path, close=False)

    return ax


def plot_model_metrics_table(
    metrics_dict: Dict[str, Dict[str, float]],
    ax: Optional[plt.Axes] = None,
    highlight_best: bool = True
) -> plt.Axes:
    """
    Create formatted table of model metrics.
    Args:
        metrics_dict: Dictionary of {model_name: {metric_name: value}}
        ax: Matplotlib axis (if None, creates new figure)
        highlight_best: Whether to highlight best values

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    ax.axis('off')

    df = pd.DataFrame(metrics_dict).T

    formatted_data = []
    for model in df.index:
        row = [model]
        for metric in df.columns:
            value = df.loc[model, metric]
            fmt = config.METRICS.get(metric.lower(), {'format': '.3f'})['format']
            row.append(f'{value:{fmt}}')
        formatted_data.append(row)

    col_labels = ['Model'] + [config.METRICS.get(m.lower(), {'name': m.upper()})['name']
                               for m in df.columns]

    table = ax.table(
        cellText=formatted_data,
        colLabels=col_labels,
        cellLoc='center',
        loc='center',
        bbox=[0, 0, 1, 1]
    )

    table.auto_set_font_size(False)
    table.set_fontsize(config.FONT_SIZES['legend'])
    table.scale(1, 2)

    for i in range(len(col_labels)):
        table[(0, i)].set_facecolor('#2C3E50')
        table[(0, i)].set_text_props(weight='bold', color='white')

    if highlight_best:
        for col_idx, metric in enumerate(df.columns, start=1):
            best_idx = df[metric].idxmin()  
            row_idx = list(df.index).index(best_idx) + 1
            table[(row_idx, col_idx)].set_facecolor('#d4edda')
            table[(row_idx, col_idx)].set_text_props(weight='bold')

    return ax


# MODEL LEADERBOARD

def plot_model_leaderboard(
    leaderboard_df: pd.DataFrame,
    score_column: str = 'score_val',
    model_column: str = 'model',
    top_n: int = 10,
    ax: Optional[plt.Axes] = None,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Axes:
    """
    Create horizontal bar chart showing model ranking.
    
    Args:
        leaderboard_df: DataFrame with model scores
        score_column: Column name containing scores
        model_column: Column name containing model names
        top_n: Number of top models to show
        ax: Matplotlib axis (if None, creates new figure)
        output_path: Path to save figure

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    top_models = leaderboard_df.nlargest(top_n, score_column)

    bars = ax.barh(
        top_models[model_column],
        top_models[score_column],
        color=config.COLORS['forecast'],
        alpha=0.7,
        edgecolor='black',
        linewidth=0.5
    )

    bars[0].set_color(config.MODEL_COLORS['autogluon'])
    bars[0].set_alpha(0.9)

    ax.set_xlabel('Validation Score (higher is better)',
                  fontweight='bold',
                  fontsize=config.FONT_SIZES['axis_label'])
    ax.set_title('Model Leaderboard',
                 fontweight='bold',
                 fontsize=config.FONT_SIZES['title'])
    ax.invert_yaxis()
    utils.add_grid(ax)

    plt.tight_layout()

    if output_path:
        utils.save_plot(ax.figure, output_path, close=False)

    return ax


# SIDE-BY-SIDE FORECAST COMPARISON

def plot_models_side_by_side(
    predictions_dict: Dict[str, pd.DataFrame],
    sensor_id: str,
    actual_data: Optional[pd.DataFrame] = None,
    n_cols: int = 3,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Figure:
    """
    Plot forecasts from multiple models side by side.

    Args:
        predictions_dict: Dictionary of {model_name: predictions_df}
                         Each df must have columns ['item_id', 'timestamp', 'mean' or 'prediction']
        sensor_id: Sensor to plot
        actual_data: Optional actual values to overlay
        n_cols: Number of columns in grid layout
        output_path: Path to save figure

    Returns:
        plt.Figure: Matplotlib figure object
    """
    utils.setup_plot_style()

    n_models = len(predictions_dict)
    n_rows = (n_models + n_cols - 1) // n_cols

    figsize = (6 * n_cols, 5 * n_rows)
    fig, axes = plt.subplots(1, n_models, figsize=figsize)

    if n_models == 1:
        axes = [axes]

    for idx, (model_name, pred_df) in enumerate(predictions_dict.items()):
        ax = axes[idx]

        sensor_data = pred_df[pred_df['item_id'] == sensor_id].sort_values('timestamp')

        pred_col = 'mean' if 'mean' in sensor_data.columns else 'prediction'
        
        color = config.get_model_color(model_name)
        marker = 'o' if idx == 0 else ('s' if idx == 1 else '^')

        ax.plot(
            sensor_data['timestamp'],
            sensor_data[pred_col],
            marker=marker,
            linestyle='-',
            label=model_name,
            color=color,
            linewidth=config.LINE_SETTINGS['linewidth'],
            markersize=config.LINE_SETTINGS['marker_size'],
            alpha=0.8
        )

        # if data provided
        if actual_data is not None:
            actual_sensor = actual_data[actual_data['item_id'] == sensor_id].sort_values('timestamp')
            if len(actual_sensor) > 0:
                ax.plot(
                    actual_sensor['timestamp'],
                    actual_sensor['value'],
                    'k--',
                    label='Actual',
                    linewidth=1.5,
                    alpha=0.6
                )

        utils.format_axis(
            ax,
            title=model_name,
            xlabel='Time',
            ylabel='Predicted Value',
            add_legend=True,
            grid=True,
            time_axis=True
        )

    plt.suptitle(
        f'24-Hour Forecast Comparison - {sensor_id}',
        fontweight='bold',
        fontsize=config.FONT_SIZES['title'] + 2,
        y=1.02
    )
    plt.tight_layout()

    if output_path:
        utils.save_plot(fig, output_path, close=False)

    return fig


def plot_models_overlay(
    predictions_dict: Dict[str, pd.DataFrame],
    sensor_id: str,
    actual_data: Optional[pd.DataFrame] = None,
    ax: Optional[plt.Axes] = None,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Axes:
    """
    Overlay multiple model forecasts on single plot.

    Args:
        predictions_dict: Dictionary of {model_name: predictions_df}
        sensor_id: Sensor to plot
        actual_data: Optional actual values
        ax: Matplotlib axis (if None, creates new figure)
        output_path: Path to save figure

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    markers = ['o', 's', '^', 'D', 'v', '<', '>', 'p']

    for idx, (model_name, pred_df) in enumerate(predictions_dict.items()):
        sensor_data = pred_df[pred_df['item_id'] == sensor_id].sort_values('timestamp')

        pred_col = 'mean' if 'mean' in sensor_data.columns else 'prediction'
        color = config.get_model_color(model_name)
        marker = markers[idx % len(markers)]

        ax.plot(
            sensor_data['timestamp'],
            sensor_data[pred_col],
            marker=marker,
            linestyle='-',
            label=model_name,
            color=color,
            linewidth=config.LINE_SETTINGS['linewidth'],
            markersize=config.LINE_SETTINGS['marker_size'],
            alpha=0.8
        )

    # if data provided
    if actual_data is not None:
        actual_sensor = actual_data[actual_data['item_id'] == sensor_id].sort_values('timestamp')
        if len(actual_sensor) > 0:
            ax.plot(
                actual_sensor['timestamp'],
                actual_sensor['value'],
                'k--',
                label='Actual',
                linewidth=2,
                alpha=0.7
            )

    utils.format_axis(
        ax,
        title=f'Model Comparison - {sensor_id}',
        xlabel='Time',
        ylabel='Value',
        add_legend=True,
        grid=True,
        time_axis=True
    )

    if output_path:
        utils.save_plot(ax.figure, output_path, close=False)

    return ax


# FORECAST DISTRIBUTION COMPARISON

def plot_forecast_distributions(
    predictions_dict: Dict[str, pd.DataFrame],
    ax: Optional[plt.Axes] = None,
    show_stats: bool = True,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Axes:
    """
    Box plot comparing forecast distributions across models.

    Args:
        predictions_dict: Dictionary of {model_name: predictions_df}
        ax: Matplotlib axis (if None, creates new figure)
        show_stats: Whether to show mean markers
        output_path: Path to save figure

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['comparison'])
        
    data = []
    labels = []
    colors = []

    for model_name, pred_df in predictions_dict.items():
        pred_col = 'mean' if 'mean' in pred_df.columns else 'prediction'
        data.append(pred_df[pred_col].values)
        labels.append(model_name)
        colors.append(config.get_model_color(model_name))

    bp = ax.boxplot(
        data,
        labels=labels,
        patch_artist=True,
        showmeans=show_stats,
        meanprops=dict(marker='D', markerfacecolor='red', markersize=8)
    )

    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.6)
        patch.set_edgecolor('black')
        patch.set_linewidth(1)

    ax.set_ylabel('Predicted Value', fontweight='bold', fontsize=config.FONT_SIZES['axis_label'])
    ax.set_title('Forecast Distribution Comparison', fontweight='bold', fontsize=config.FONT_SIZES['title'])
    utils.add_grid(ax)

    plt.tight_layout()

    if output_path:
        utils.save_plot(ax.figure, output_path, close=False)

    return ax


# TRAINING HISTORY COMPARISON

def plot_training_history(
    history_df: pd.DataFrame,
    loss_column: str = 'loss',
    val_loss_column: Optional[str] = 'val_loss',
    model_name: str = 'Model',
    ax: Optional[plt.Axes] = None,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Axes:
    """
    Plot training loss history for a model.

    Args:
        history_df: DataFrame with training history
        loss_column: Column name for training loss
        val_loss_column: Column name for validation loss (optional)
        model_name: Name of the model
        ax: Matplotlib axis (if None, creates new figure)
        output_path: Path to save figure

    Returns:
        plt.Axes: Matplotlib axis object
    """
    if ax is None:
        fig, ax = utils.create_figure(figsize=config.FIGSIZE['single'])

    epochs = range(1, len(history_df) + 1)

    color = config.get_model_color(model_name)
    ax.plot(
        epochs,
        history_df[loss_column],
        'o-',
        linewidth=config.LINE_SETTINGS['linewidth'],
        markersize=6,
        color=color,
        label='Training Loss'
    )

    if val_loss_column and val_loss_column in history_df.columns:
        ax.plot(
            epochs,
            history_df[val_loss_column],
            's-',
            linewidth=config.LINE_SETTINGS['linewidth'],
            markersize=6,
            color=config.COLORS['actual'],
            label='Validation Loss'
        )

    improvement = ((history_df[loss_column].iloc[0] - history_df[loss_column].iloc[-1]) /
                   history_df[loss_column].iloc[0]) * 100

    ax.text(
        0.98, 0.98,
        f'Improvement: {improvement:.1f}%',
        transform=ax.transAxes,
        ha='right',
        va='top',
        fontsize=config.FONT_SIZES['annotation'],
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5)
    )

    utils.format_axis(
        ax,
        title=f'{model_name} Training Convergence',
        xlabel='Epoch',
        ylabel='Loss',
        add_legend=True,
        grid=True
    )

    plt.tight_layout()

    if output_path:
        utils.save_plot(ax.figure, output_path, close=False)

    return ax


# COMPARISON DASHBOARD

def create_comparison_dashboard(
    predictions_dict: Dict[str, pd.DataFrame],
    metrics_dict: Dict[str, Dict[str, float]],
    sensor_id: str,
    actual_data: Optional[pd.DataFrame] = None,
    output_path: Optional[Union[str, Path]] = None
) -> plt.Figure:
    """
    Create comprehensive model comparison dashboard.

    Includes:
    1. Model performance comparison (bar chart)
    2. Forecast distributions (box plot)
    3. Side-by-side forecasts
    4. Metrics table

    Args:
        predictions_dict: Dictionary of {model_name: predictions_df}
        metrics_dict: Dictionary of {model_name: {metric: value}}
        sensor_id: Sensor to visualize
        actual_data: Optional actual values
        output_path: Path to save figure

    Returns:
        plt.Figure: Matplotlib figure with multiple subplots
    """
    utils.setup_plot_style()

    fig = plt.figure(figsize=config.FIGSIZE['dashboard'])
    gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)

    ax1 = fig.add_subplot(gs[0, 0])
    plot_model_performance_comparison(metrics_dict, ax=ax1)

    ax2 = fig.add_subplot(gs[0, 1])
    plot_forecast_distributions(predictions_dict, ax=ax2)

    ax3 = fig.add_subplot(gs[1, 0])
    plot_models_overlay(predictions_dict, sensor_id, actual_data, ax=ax3)

    ax4 = fig.add_subplot(gs[1, 1])
    plot_model_metrics_table(metrics_dict, ax=ax4)

    fig.suptitle(
        'Model Comparison Dashboard',
        fontsize=config.FONT_SIZES['title'] + 2,
        fontweight='bold',
        y=0.98
    )

    if output_path:
        utils.save_plot(fig, output_path, close=False)

    return fig
