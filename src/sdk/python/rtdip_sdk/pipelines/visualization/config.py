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
Standardized visualization configuration for RTDIP time series forecasting.

This module defines standard colors, styles, and settings to ensure consistent
visualizations across all forecasting, anomaly detection, and model comparison tasks.

Supports both Matplotlib (static) and Plotly (interactive) backends.

Example
--------
```python
from rtdip_sdk.pipelines.visualization import config

# Use predefined colors
historical_color = config.COLORS['historical']

# Get model-specific color
model_color = config.get_model_color('autogluon')

# Get figure size for grid
figsize = config.get_figsize_for_grid(6)
```
"""

from typing import Dict, Tuple

# BACKEND CONFIGURATION
VISUALIZATION_BACKEND: str = "matplotlib"  # Options: 'matplotlib' or 'plotly'

# COLOR SCHEMES

# Primary colors for different data types
COLORS: Dict[str, str] = {
    # Time series data
    "historical": "#2C3E50",  # historical data
    "forecast": "#27AE60",  # predictions
    "actual": "#2980B9",  # ground truth
    "anomaly": "#E74C3C",  # anomalies/errors
    # Confidence intervals
    "ci_60": "#27AE60",  # alpha=0.3
    "ci_80": "#27AE60",  # alpha=0.15
    "ci_90": "#27AE60",  # alpha=0.1
    # Special markers
    "forecast_start": "#E74C3C",  # forecast start line
    "threshold": "#F39C12",  # thresholds
}

# Model-specific colors (for comparison plots)
MODEL_COLORS: Dict[str, str] = {
    "autogluon": "#2ECC71",
    "lstm": "#E74C3C",
    "xgboost": "#3498DB",
    "arima": "#9B59B6",
    "prophet": "#F39C12",
    "ensemble": "#1ABC9C",
}

# Confidence interval alpha values
CI_ALPHA: Dict[int, float] = {
    60: 0.3,  # 60% - most opaque
    80: 0.2,  # 80% - medium
    90: 0.1,  # 90% - most transparent
}

# FIGURE SIZES

FIGSIZE: Dict[str, Tuple[float, float]] = {
    "single": (12, 6),  # Single time series plot
    "single_tall": (12, 8),  # Single plot with more vertical space
    "comparison": (14, 6),  # Side-by-side comparison
    "grid_small": (14, 8),  # 2-3 subplot grid
    "grid_medium": (16, 10),  # 4-6 subplot grid
    "grid_large": (18, 12),  # 6-9 subplot grid
    "dashboard": (20, 16),  # Full dashboard with 9+ subplots
    "wide": (16, 5),  # Wide single plot
}

# EXPORT SETTINGS

EXPORT: Dict[str, any] = {
    "dpi": 300,  # High resolution
    "format": "png",  # Default format
    "bbox_inches": "tight",  # Tight bounding box
    "facecolor": "white",  # White background
    "edgecolor": "none",  # No edge color
}

# STYLE SETTINGS

STYLE: str = "seaborn-v0_8-whitegrid"

FONT_SIZES: Dict[str, int] = {
    "title": 14,
    "subtitle": 12,
    "axis_label": 12,
    "tick_label": 10,
    "legend": 10,
    "annotation": 9,
}

LINE_SETTINGS: Dict[str, float] = {
    "linewidth": 2.0,  # Default line width
    "linewidth_thin": 1.5,  # Thin lines (for CI, grids)
    "marker_size": 4,  # Default marker size for line plots
    "scatter_size": 80,  # Scatter plot marker size
    "anomaly_size": 100,  # Anomaly marker size
}

GRID: Dict[str, any] = {
    "alpha": 0.3,  # Grid transparency
    "linestyle": "--",  # Dashed grid lines
    "linewidth": 0.5,  # Thin grid lines
}

TIME_FORMATS: Dict[str, str] = {
    "hourly": "%Y-%m-%d %H:%M",
    "daily": "%Y-%m-%d",
    "monthly": "%Y-%m",
    "display": "%m/%d %H:%M",
}

METRICS: Dict[str, Dict[str, str]] = {
    "mae": {"name": "MAE", "format": ".3f"},
    "mse": {"name": "MSE", "format": ".3f"},
    "rmse": {"name": "RMSE", "format": ".3f"},
    "mape": {"name": "MAPE (%)", "format": ".2f"},
    "smape": {"name": "SMAPE (%)", "format": ".2f"},
    "r2": {"name": "R²", "format": ".4f"},
    "mae_p50": {"name": "MAE (P50)", "format": ".3f"},
    "mae_p90": {"name": "MAE (P90)", "format": ".3f"},
}

# Metric display order (left to right, top to bottom)
METRIC_ORDER: list = ["mae", "rmse", "mse", "mape", "smape", "r2"]

# OUTPUT DIRECTORY SETTINGS
DEFAULT_OUTPUT_DIR: str = "output_images"

# COLORBLIND-FRIENDLY PALETTE

COLORBLIND_PALETTE: list = [
    "#0173B2",
    "#DE8F05",
    "#029E73",
    "#CC78BC",
    "#CA9161",
    "#949494",
    "#ECE133",
    "#56B4E9",
]


# HELPER FUNCTIONS


def get_grid_layout(n_plots: int) -> Tuple[int, int]:
    """
    Calculate optimal subplot grid layout (rows, cols) for n_plots.

    Prioritizes 3 columns for better horizontal space usage.

    Args:
        n_plots: Number of subplots needed

    Returns:
        Tuple of (n_rows, n_cols)

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.config import get_grid_layout

    rows, cols = get_grid_layout(5)  # Returns (2, 3)
    ```
    """
    if n_plots <= 0:
        return (0, 0)
    elif n_plots == 1:
        return (1, 1)
    elif n_plots == 2:
        return (1, 2)
    elif n_plots <= 3:
        return (1, 3)
    elif n_plots <= 6:
        return (2, 3)
    elif n_plots <= 9:
        return (3, 3)
    elif n_plots <= 12:
        return (4, 3)
    else:
        n_cols = 3
        n_rows = (n_plots + n_cols - 1) // n_cols
        return (n_rows, n_cols)


def get_model_color(model_name: str) -> str:
    """
    Get color for a specific model, with fallback to colorblind palette.

    Args:
        model_name: Model name (case-insensitive)

    Returns:
        Hex color code string

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.config import get_model_color

    color = get_model_color('AutoGluon')  # Returns '#2ECC71'
    color = get_model_color('custom_model')  # Returns color from palette
    ```
    """
    model_name_lower = model_name.lower()

    if model_name_lower in MODEL_COLORS:
        return MODEL_COLORS[model_name_lower]

    idx = hash(model_name) % len(COLORBLIND_PALETTE)
    return COLORBLIND_PALETTE[idx]


def get_figsize_for_grid(n_plots: int) -> Tuple[float, float]:
    """
    Get appropriate figure size for a grid of n plots.

    Args:
        n_plots: Number of subplots

    Returns:
        Tuple of (width, height) in inches

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.config import get_figsize_for_grid

    figsize = get_figsize_for_grid(4)  # Returns (16, 10) for grid_medium
    ```
    """
    if n_plots <= 1:
        return FIGSIZE["single"]
    elif n_plots <= 3:
        return FIGSIZE["grid_small"]
    elif n_plots <= 6:
        return FIGSIZE["grid_medium"]
    elif n_plots <= 9:
        return FIGSIZE["grid_large"]
    else:
        return FIGSIZE["dashboard"]
