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
Matplotlib-based forecasting visualization components.

This module provides class-based visualization components for time series
forecasting results, including confidence intervals, model comparisons,
and error analysis.

Example
--------
```python
from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import ForecastPlot
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

plot = ForecastPlot(
    historical_data=historical_df,
    forecast_data=forecast_df,
    forecast_start=pd.Timestamp('2024-01-05'),
    sensor_id='SENSOR_001'
)
fig = plot.plot()
plot.save('forecast.png')
```
"""

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
    validate_dataframe,
    coerce_types,
    prepare_dataframe,
    check_data_overlap,
)

warnings.filterwarnings("ignore")


class ForecastPlot(MatplotlibVisualizationInterface):
    """
    Plot time series forecast with confidence intervals.

    This component creates a visualization showing historical data,
    forecast predictions, and optional confidence interval bands.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import ForecastPlot
    import pandas as pd

    historical_df = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=100, freq='h'),
        'value': [1.0] * 100
    })
    forecast_df = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-05', periods=24, freq='h'),
        'mean': [1.5] * 24,
        '0.1': [1.0] * 24,
        '0.9': [2.0] * 24,
    })

    plot = ForecastPlot(
        historical_data=historical_df,
        forecast_data=forecast_df,
        forecast_start=pd.Timestamp('2024-01-05'),
        sensor_id='SENSOR_001',
        ci_levels=[60, 80]
    )
    fig = plot.plot()
    plot.save('forecast.png')
    ```

    Parameters:
        historical_data (PandasDataFrame): DataFrame with 'timestamp' and 'value' columns.
        forecast_data (PandasDataFrame): DataFrame with 'timestamp', 'mean', and
            quantile columns ('0.1', '0.2', '0.8', '0.9').
        forecast_start (pd.Timestamp): Timestamp marking the start of forecast period.
        sensor_id (str, optional): Sensor identifier for the plot title.
        lookback_hours (int, optional): Hours of historical data to show. Defaults to 168.
        ci_levels (List[int], optional): Confidence interval levels. Defaults to [60, 80].
        title (str, optional): Custom plot title.
        show_legend (bool, optional): Whether to show legend. Defaults to True.
        column_mapping (Dict[str, str], optional): Mapping from your column names to
            expected names. Example: {"time": "timestamp", "reading": "value"}
    """

    historical_data: PandasDataFrame
    forecast_data: PandasDataFrame
    forecast_start: pd.Timestamp
    sensor_id: Optional[str]
    lookback_hours: int
    ci_levels: List[int]
    title: Optional[str]
    show_legend: bool
    column_mapping: Optional[Dict[str, str]]
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        historical_data: PandasDataFrame,
        forecast_data: PandasDataFrame,
        forecast_start: pd.Timestamp,
        sensor_id: Optional[str] = None,
        lookback_hours: int = 168,
        ci_levels: Optional[List[int]] = None,
        title: Optional[str] = None,
        show_legend: bool = True,
        column_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        self.column_mapping = column_mapping
        self.sensor_id = sensor_id
        self.lookback_hours = lookback_hours
        self.ci_levels = ci_levels if ci_levels is not None else [60, 80]
        self.title = title
        self.show_legend = show_legend
        self._fig = None
        self._ax = None

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

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the forecast visualization.

        Args:
            ax: Optional matplotlib axis to plot on. If None, creates new figure.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        if ax is None:
            self._fig, self._ax = utils.create_figure(figsize=config.FIGSIZE["single"])
        else:
            self._ax = ax
            self._fig = ax.figure

        self._ax.plot(
            self.historical_data["timestamp"],
            self.historical_data["value"],
            "o-",
            color=config.COLORS["historical"],
            label="Historical Data",
            linewidth=config.LINE_SETTINGS["linewidth"],
            markersize=config.LINE_SETTINGS["marker_size"],
            alpha=0.8,
        )

        self._ax.plot(
            self.forecast_data["timestamp"],
            self.forecast_data["mean"],
            "s-",
            color=config.COLORS["forecast"],
            label="Forecast (mean)",
            linewidth=config.LINE_SETTINGS["linewidth"],
            markersize=config.LINE_SETTINGS["marker_size"],
            alpha=0.9,
        )

        for ci_level in sorted(self.ci_levels, reverse=True):
            if (
                ci_level == 60
                and "0.2" in self.forecast_data.columns
                and "0.8" in self.forecast_data.columns
            ):
                utils.plot_confidence_intervals(
                    self._ax,
                    self.forecast_data["timestamp"],
                    self.forecast_data["0.2"],
                    self.forecast_data["0.8"],
                    ci_level=60,
                )
            elif (
                ci_level == 80
                and "0.1" in self.forecast_data.columns
                and "0.9" in self.forecast_data.columns
            ):
                utils.plot_confidence_intervals(
                    self._ax,
                    self.forecast_data["timestamp"],
                    self.forecast_data["0.1"],
                    self.forecast_data["0.9"],
                    ci_level=80,
                )
            elif (
                ci_level == 90
                and "0.05" in self.forecast_data.columns
                and "0.95" in self.forecast_data.columns
            ):
                utils.plot_confidence_intervals(
                    self._ax,
                    self.forecast_data["timestamp"],
                    self.forecast_data["0.05"],
                    self.forecast_data["0.95"],
                    ci_level=90,
                )

        utils.add_vertical_line(self._ax, self.forecast_start, label="Forecast Start")

        plot_title = self.title
        if plot_title is None and self.sensor_id:
            plot_title = f"{self.sensor_id} - Forecast with Confidence Intervals"
        elif plot_title is None:
            plot_title = "Time Series Forecast with Confidence Intervals"

        utils.format_axis(
            self._ax,
            title=plot_title,
            xlabel="Time",
            ylabel="Value",
            add_legend=self.show_legend,
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
        """
        Save the visualization to file.

        Args:
            filepath: Output file path
            dpi: DPI for output image
            **kwargs: Additional save options

        Returns:
            Path to the saved file
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


class ForecastComparisonPlot(MatplotlibVisualizationInterface):
    """
    Plot forecast against actual values for comparison.

    This component creates a visualization comparing forecast predictions
    with actual ground truth values.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import ForecastComparisonPlot

    plot = ForecastComparisonPlot(
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
        lookback_hours (int, optional): Hours of historical data to show. Defaults to 168.
        column_mapping (Dict[str, str], optional): Mapping from your column names to
            expected names.
    """

    historical_data: PandasDataFrame
    forecast_data: PandasDataFrame
    actual_data: PandasDataFrame
    forecast_start: pd.Timestamp
    sensor_id: Optional[str]
    lookback_hours: int
    column_mapping: Optional[Dict[str, str]]
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        historical_data: PandasDataFrame,
        forecast_data: PandasDataFrame,
        actual_data: PandasDataFrame,
        forecast_start: pd.Timestamp,
        sensor_id: Optional[str] = None,
        lookback_hours: int = 168,
        column_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        self.column_mapping = column_mapping
        self.sensor_id = sensor_id
        self.lookback_hours = lookback_hours
        self._fig = None
        self._ax = None

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

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the forecast comparison visualization.

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

        self._ax.plot(
            self.historical_data["timestamp"],
            self.historical_data["value"],
            "o-",
            color=config.COLORS["historical"],
            label="Historical",
            linewidth=config.LINE_SETTINGS["linewidth"],
            markersize=config.LINE_SETTINGS["marker_size"],
            alpha=0.7,
        )

        self._ax.plot(
            self.actual_data["timestamp"],
            self.actual_data["value"],
            "o-",
            color=config.COLORS["actual"],
            label="Actual",
            linewidth=config.LINE_SETTINGS["linewidth"],
            markersize=config.LINE_SETTINGS["marker_size"],
            alpha=0.8,
        )

        self._ax.plot(
            self.forecast_data["timestamp"],
            self.forecast_data["mean"],
            "s-",
            color=config.COLORS["forecast"],
            label="Forecast",
            linewidth=config.LINE_SETTINGS["linewidth"],
            markersize=config.LINE_SETTINGS["marker_size"],
            alpha=0.9,
        )

        utils.add_vertical_line(self._ax, self.forecast_start, label="Forecast Start")

        title = (
            f"{self.sensor_id} - Forecast vs Actual"
            if self.sensor_id
            else "Forecast vs Actual Values"
        )
        utils.format_axis(
            self._ax,
            title=title,
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


class MultiSensorForecastPlot(MatplotlibVisualizationInterface):
    """
    Create multi-sensor overview plot in grid layout.

    This component creates a grid visualization showing forecasts
    for multiple sensors simultaneously.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import MultiSensorForecastPlot

    plot = MultiSensorForecastPlot(
        predictions_df=predictions,
        historical_df=historical,
        lookback_hours=168,
        max_sensors=9
    )
    fig = plot.plot()
    ```

    Parameters:
        predictions_df (PandasDataFrame): DataFrame with columns
            ['item_id', 'timestamp', 'mean', ...].
        historical_df (PandasDataFrame): DataFrame with columns
            ['TagName', 'EventTime', 'Value'].
        lookback_hours (int, optional): Hours of historical data to show. Defaults to 168.
        max_sensors (int, optional): Maximum number of sensors to plot.
        predictions_column_mapping (Dict[str, str], optional): Mapping for predictions DataFrame.
            Default expected columns: 'item_id', 'timestamp', 'mean'
        historical_column_mapping (Dict[str, str], optional): Mapping for historical DataFrame.
            Default expected columns: 'TagName', 'EventTime', 'Value'
    """

    predictions_df: PandasDataFrame
    historical_df: PandasDataFrame
    lookback_hours: int
    max_sensors: Optional[int]
    predictions_column_mapping: Optional[Dict[str, str]]
    historical_column_mapping: Optional[Dict[str, str]]
    _fig: Optional[plt.Figure]

    def __init__(
        self,
        predictions_df: PandasDataFrame,
        historical_df: PandasDataFrame,
        lookback_hours: int = 168,
        max_sensors: Optional[int] = None,
        predictions_column_mapping: Optional[Dict[str, str]] = None,
        historical_column_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        self.lookback_hours = lookback_hours
        self.max_sensors = max_sensors
        self.predictions_column_mapping = predictions_column_mapping
        self.historical_column_mapping = historical_column_mapping
        self._fig = None

        ci_columns = ["0.05", "0.1", "0.2", "0.8", "0.9", "0.95"]
        self.predictions_df = prepare_dataframe(
            predictions_df,
            required_columns=["item_id", "timestamp", "mean"],
            df_name="predictions_df",
            column_mapping=predictions_column_mapping,
            datetime_cols=["timestamp"],
            numeric_cols=["mean"] + ci_columns,
            optional_columns=ci_columns,
        )

        self.historical_df = prepare_dataframe(
            historical_df,
            required_columns=["TagName", "EventTime", "Value"],
            df_name="historical_df",
            column_mapping=historical_column_mapping,
            datetime_cols=["EventTime"],
            numeric_cols=["Value"],
        )

    def plot(self) -> plt.Figure:
        """
        Generate the multi-sensor overview visualization.

        Returns:
            matplotlib.figure.Figure: The generated figure.
        """
        utils.setup_plot_style()

        sensors = self.predictions_df["item_id"].unique()
        if self.max_sensors:
            sensors = sensors[: self.max_sensors]

        n_sensors = len(sensors)
        if n_sensors == 0:
            raise VisualizationDataError(
                "No sensors found in predictions_df. "
                "Check that 'item_id' column contains valid sensor identifiers."
            )

        n_rows, n_cols = config.get_grid_layout(n_sensors)
        figsize = config.get_figsize_for_grid(n_sensors)

        self._fig, axes = plt.subplots(n_rows, n_cols, figsize=figsize)
        if n_sensors == 1:
            axes = np.array([axes])
        axes = axes.flatten()

        for idx, sensor in enumerate(sensors):
            ax = axes[idx]

            sensor_preds = self.predictions_df[
                self.predictions_df["item_id"] == sensor
            ].copy()
            sensor_preds = sensor_preds.sort_values("timestamp")

            if len(sensor_preds) == 0:
                ax.text(
                    0.5,
                    0.5,
                    f"No data for {sensor}",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                )
                ax.set_title(sensor[:40], fontsize=config.FONT_SIZES["subtitle"])
                continue

            forecast_start = sensor_preds["timestamp"].min()

            sensor_hist = self.historical_df[
                self.historical_df["TagName"] == sensor
            ].copy()
            sensor_hist = sensor_hist.sort_values("EventTime")
            cutoff_time = forecast_start - pd.Timedelta(hours=self.lookback_hours)
            sensor_hist = sensor_hist[
                (sensor_hist["EventTime"] >= cutoff_time)
                & (sensor_hist["EventTime"] < forecast_start)
            ]

            historical_data = pd.DataFrame(
                {"timestamp": sensor_hist["EventTime"], "value": sensor_hist["Value"]}
            )

            forecast_plot = ForecastPlot(
                historical_data=historical_data,
                forecast_data=sensor_preds,
                forecast_start=forecast_start,
                sensor_id=sensor[:40],
                lookback_hours=self.lookback_hours,
                show_legend=(idx == 0),
            )
            forecast_plot.plot(ax=ax)

        utils.hide_unused_subplots(axes, n_sensors)

        plt.suptitle(
            "Forecasts - All Sensors",
            fontsize=config.FONT_SIZES["title"] + 2,
            fontweight="bold",
            y=1.0,
        )
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


class ResidualPlot(MatplotlibVisualizationInterface):
    """
    Plot residuals (actual - predicted) over time.

    This component visualizes the forecast errors over time to identify
    systematic biases or patterns in the predictions.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import ResidualPlot

    plot = ResidualPlot(
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
    """

    actual: pd.Series
    predicted: pd.Series
    timestamps: pd.Series
    sensor_id: Optional[str]
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        actual: pd.Series,
        predicted: pd.Series,
        timestamps: pd.Series,
        sensor_id: Optional[str] = None,
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
        self._fig = None
        self._ax = None

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the residuals visualization.

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

        residuals = self.actual - self.predicted

        self._ax.plot(
            self.timestamps,
            residuals,
            "o-",
            color=config.COLORS["actual"],
            linewidth=config.LINE_SETTINGS["linewidth_thin"],
            markersize=config.LINE_SETTINGS["marker_size"],
            alpha=0.7,
        )

        self._ax.axhline(
            0, color="black", linestyle="--", linewidth=1.5, alpha=0.5, label="Zero Error"
        )

        mean_residual = residuals.mean()
        self._ax.axhline(
            mean_residual,
            color=config.COLORS["anomaly"],
            linestyle=":",
            linewidth=1.5,
            alpha=0.7,
            label=f"Mean Residual: {mean_residual:.3f}",
        )

        title = (
            f"{self.sensor_id} - Residuals Over Time"
            if self.sensor_id
            else "Residuals Over Time"
        )
        utils.format_axis(
            self._ax,
            title=title,
            xlabel="Time",
            ylabel="Residual (Actual - Predicted)",
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


class ErrorDistributionPlot(MatplotlibVisualizationInterface):
    """
    Plot histogram of forecast errors.

    This component visualizes the distribution of forecast errors
    to understand the error characteristics.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import ErrorDistributionPlot

    plot = ErrorDistributionPlot(
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
        bins (int, optional): Number of histogram bins. Defaults to 30.
    """

    actual: pd.Series
    predicted: pd.Series
    sensor_id: Optional[str]
    bins: int
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        actual: pd.Series,
        predicted: pd.Series,
        sensor_id: Optional[str] = None,
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
        self.bins = bins
        self._fig = None
        self._ax = None

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the error distribution visualization.

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

        errors = self.actual - self.predicted

        self._ax.hist(
            errors,
            bins=self.bins,
            color=config.COLORS["actual"],
            alpha=0.7,
            edgecolor="black",
            linewidth=0.5,
        )

        mean_error = errors.mean()
        median_error = errors.median()

        self._ax.axvline(
            mean_error, color="red", linestyle="--", linewidth=2, label=f"Mean: {mean_error:.3f}"
        )
        self._ax.axvline(
            median_error,
            color="orange",
            linestyle="--",
            linewidth=2,
            label=f"Median: {median_error:.3f}",
        )
        self._ax.axvline(0, color="black", linestyle="-", linewidth=1.5, alpha=0.5)

        std_error = errors.std()
        stats_text = f"Std: {std_error:.3f}\nMAE: {np.abs(errors).mean():.3f}"
        self._ax.text(
            0.98,
            0.98,
            stats_text,
            transform=self._ax.transAxes,
            verticalalignment="top",
            horizontalalignment="right",
            bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
            fontsize=config.FONT_SIZES["annotation"],
        )

        title = (
            f"{self.sensor_id} - Error Distribution"
            if self.sensor_id
            else "Forecast Error Distribution"
        )
        utils.format_axis(
            self._ax,
            title=title,
            xlabel="Error (Actual - Predicted)",
            ylabel="Frequency",
            add_legend=True,
            grid=True,
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


class ScatterPlot(MatplotlibVisualizationInterface):
    """
    Scatter plot of actual vs predicted values.

    This component visualizes the relationship between actual and
    predicted values to assess model performance.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import ScatterPlot

    plot = ScatterPlot(
        actual=actual_series,
        predicted=predicted_series,
        sensor_id='SENSOR_001',
        show_metrics=True
    )
    fig = plot.plot()
    ```

    Parameters:
        actual (pd.Series): Actual values.
        predicted (pd.Series): Predicted values.
        sensor_id (str, optional): Sensor identifier for the plot title.
        show_metrics (bool, optional): Whether to show metrics. Defaults to True.
    """

    actual: pd.Series
    predicted: pd.Series
    sensor_id: Optional[str]
    show_metrics: bool
    _fig: Optional[plt.Figure]
    _ax: Optional[plt.Axes]

    def __init__(
        self,
        actual: pd.Series,
        predicted: pd.Series,
        sensor_id: Optional[str] = None,
        show_metrics: bool = True,
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
        self.show_metrics = show_metrics
        self._fig = None
        self._ax = None

    def plot(self, ax: Optional[plt.Axes] = None) -> plt.Figure:
        """
        Generate the scatter plot visualization.

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

        self._ax.scatter(
            self.actual,
            self.predicted,
            alpha=0.6,
            s=config.LINE_SETTINGS["scatter_size"],
            color=config.COLORS["actual"],
            edgecolors="black",
            linewidth=0.5,
        )

        min_val = min(self.actual.min(), self.predicted.min())
        max_val = max(self.actual.max(), self.predicted.max())
        self._ax.plot(
            [min_val, max_val],
            [min_val, max_val],
            "r--",
            linewidth=2,
            label="Perfect Prediction",
            alpha=0.7,
        )

        if self.show_metrics:
            try:
                from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

                r2 = r2_score(self.actual, self.predicted)
                rmse = np.sqrt(mean_squared_error(self.actual, self.predicted))
                mae = mean_absolute_error(self.actual, self.predicted)
            except ImportError:
                errors = self.actual - self.predicted
                mae = np.abs(errors).mean()
                rmse = np.sqrt((errors**2).mean())
                ss_res = np.sum(errors**2)
                ss_tot = np.sum((self.actual - self.actual.mean()) ** 2)
                r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

            metrics_text = f"R² = {r2:.4f}\nRMSE = {rmse:.3f}\nMAE = {mae:.3f}"
            self._ax.text(
                0.05,
                0.95,
                metrics_text,
                transform=self._ax.transAxes,
                verticalalignment="top",
                bbox=dict(boxstyle="round", facecolor="white", alpha=0.8),
                fontsize=config.FONT_SIZES["annotation"],
            )

        title = (
            f"{self.sensor_id} - Actual vs Predicted"
            if self.sensor_id
            else "Actual vs Predicted Values"
        )
        utils.format_axis(
            self._ax,
            title=title,
            xlabel="Actual Value",
            ylabel="Predicted Value",
            add_legend=True,
            grid=True,
        )

        self._ax.set_aspect("equal", adjustable="datalim")

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


class ForecastDashboard(MatplotlibVisualizationInterface):
    """
    Create comprehensive forecast dashboard with multiple views.

    This component creates a dashboard including forecast with confidence
    intervals, forecast vs actual, residuals, error distribution, scatter
    plot, and metrics table.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import ForecastDashboard

    dashboard = ForecastDashboard(
        historical_data=historical_df,
        forecast_data=forecast_df,
        actual_data=actual_df,
        forecast_start=pd.Timestamp('2024-01-05'),
        sensor_id='SENSOR_001'
    )
    fig = dashboard.plot()
    dashboard.save('dashboard.png')
    ```

    Parameters:
        historical_data (PandasDataFrame): Historical time series data.
        forecast_data (PandasDataFrame): Forecast predictions with confidence intervals.
        actual_data (PandasDataFrame): Actual values during forecast period.
        forecast_start (pd.Timestamp): Start of forecast period.
        sensor_id (str, optional): Sensor identifier.
        column_mapping (Dict[str, str], optional): Mapping from your column names to
            expected names.
    """

    historical_data: PandasDataFrame
    forecast_data: PandasDataFrame
    actual_data: PandasDataFrame
    forecast_start: pd.Timestamp
    sensor_id: Optional[str]
    column_mapping: Optional[Dict[str, str]]
    _fig: Optional[plt.Figure]

    def __init__(
        self,
        historical_data: PandasDataFrame,
        forecast_data: PandasDataFrame,
        actual_data: PandasDataFrame,
        forecast_start: pd.Timestamp,
        sensor_id: Optional[str] = None,
        column_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        self.column_mapping = column_mapping
        self.sensor_id = sensor_id
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

    def plot(self) -> plt.Figure:
        """
        Generate the forecast dashboard.

        Returns:
            matplotlib.figure.Figure: The generated dashboard figure.
        """
        utils.setup_plot_style()

        self._fig = plt.figure(figsize=config.FIGSIZE["dashboard"])
        gs = self._fig.add_gridspec(3, 2, hspace=0.3, wspace=0.3)

        ax1 = self._fig.add_subplot(gs[0, 0])
        forecast_plot = ForecastPlot(
            self.historical_data,
            self.forecast_data,
            self.forecast_start,
            sensor_id=self.sensor_id,
        )
        forecast_plot.plot(ax=ax1)

        ax2 = self._fig.add_subplot(gs[0, 1])
        comparison_plot = ForecastComparisonPlot(
            self.historical_data,
            self.forecast_data,
            self.actual_data,
            self.forecast_start,
            sensor_id=self.sensor_id,
        )
        comparison_plot.plot(ax=ax2)

        merged = pd.merge(
            self.forecast_data[["timestamp", "mean"]],
            self.actual_data[["timestamp", "value"]],
            on="timestamp",
            how="inner",
        )

        if len(merged) > 0:
            ax3 = self._fig.add_subplot(gs[1, 0])
            residual_plot = ResidualPlot(
                merged["value"],
                merged["mean"],
                merged["timestamp"],
                sensor_id=self.sensor_id,
            )
            residual_plot.plot(ax=ax3)

            ax4 = self._fig.add_subplot(gs[1, 1])
            error_plot = ErrorDistributionPlot(
                merged["value"], merged["mean"], sensor_id=self.sensor_id
            )
            error_plot.plot(ax=ax4)

            ax5 = self._fig.add_subplot(gs[2, 0])
            scatter_plot = ScatterPlot(
                merged["value"], merged["mean"], sensor_id=self.sensor_id
            )
            scatter_plot.plot(ax=ax5)

            ax6 = self._fig.add_subplot(gs[2, 1])
            ax6.axis("off")

            errors = merged["value"] - merged["mean"]
            try:
                from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

                mae = mean_absolute_error(merged["value"], merged["mean"])
                mse = mean_squared_error(merged["value"], merged["mean"])
                rmse = np.sqrt(mse)
                r2 = r2_score(merged["value"], merged["mean"])
            except ImportError:
                mae = np.abs(errors).mean()
                mse = (errors**2).mean()
                rmse = np.sqrt(mse)
                ss_res = np.sum(errors**2)
                ss_tot = np.sum((merged["value"] - merged["value"].mean()) ** 2)
                r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0.0

            mape = (
                np.mean(np.abs((merged["value"] - merged["mean"]) / merged["value"]))
                * 100
            )

            metrics_data = [
                ["MAE", f"{mae:.4f}"],
                ["MSE", f"{mse:.4f}"],
                ["RMSE", f"{rmse:.4f}"],
                ["MAPE", f"{mape:.2f}%"],
                ["R²", f"{r2:.4f}"],
            ]

            table = ax6.table(
                cellText=metrics_data,
                colLabels=["Metric", "Value"],
                cellLoc="left",
                loc="center",
                bbox=[0.1, 0.3, 0.8, 0.6],
            )
            table.auto_set_font_size(False)
            table.set_fontsize(config.FONT_SIZES["legend"])
            table.scale(1, 2)

            for i in range(2):
                table[(0, i)].set_facecolor("#2C3E50")
                table[(0, i)].set_text_props(weight="bold", color="white")

            ax6.set_title(
                "Forecast Metrics",
                fontsize=config.FONT_SIZES["title"],
                fontweight="bold",
                pad=20,
            )
        else:
            for gs_idx in [(1, 0), (1, 1), (2, 0), (2, 1)]:
                ax = self._fig.add_subplot(gs[gs_idx])
                ax.text(
                    0.5,
                    0.5,
                    "No overlapping timestamps\nfor error analysis",
                    ha="center",
                    va="center",
                    transform=ax.transAxes,
                    fontsize=12,
                    color="red",
                )
                ax.axis("off")

        main_title = (
            f"Forecast Dashboard - {self.sensor_id}"
            if self.sensor_id
            else "Forecast Dashboard"
        )
        self._fig.suptitle(
            main_title,
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
