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

from pathlib import Path
from typing import Optional, Union

import matplotlib.pyplot as plt
from matplotlib.figure import Figure, SubFigure
from matplotlib.axes import Axes

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame

from ..interfaces import MatplotlibVisualizationInterface


class AnomalyDetectionPlot(MatplotlibVisualizationInterface):
    """
    Plot time series data with detected anomalies highlighted.

    This component visualizes the original time series data alongside detected
    anomalies, making it easy to identify and analyze outliers. Internally converts
    PySpark DataFrames to Pandas for visualization.

    Parameters:
        ts_data (SparkDataFrame): Time series data with 'timestamp' and 'value' columns
        ad_data (SparkDataFrame): Anomaly detection results with 'timestamp' and 'value' columns
        sensor_id (str, optional): Sensor identifier for the plot title
        title (str, optional): Custom plot title
        figsize (tuple, optional): Figure size as (width, height). Defaults to (18, 6)
        linewidth (float, optional): Line width for time series. Defaults to 1.6
        anomaly_marker_size (int, optional): Marker size for anomalies. Defaults to 70
        anomaly_color (str, optional): Color for anomaly markers. Defaults to 'red'
        ts_color (str, optional): Color for time series line. Defaults to 'steelblue'

    Example:
        ```python
        from rtdip_sdk.pipelines.visualization.matplotlib.anomaly_detection import AnomalyDetectionPlot

        plot = AnomalyDetectionPlot(
            ts_data=df_full_spark,
            ad_data=df_anomalies_spark,
            sensor_id='SENSOR_001'
        )

        fig = plot.plot()
        plot.save('anomalies.png')
        ```
    """

    def __init__(
        self,
        ts_data: SparkDataFrame,
        ad_data: SparkDataFrame,
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        figsize: tuple = (18, 6),
        linewidth: float = 1.6,
        anomaly_marker_size: int = 70,
        anomaly_color: str = "red",
        ts_color: str = "steelblue",
        ax: Optional[Axes] = None,
    ) -> None:
        """
        Initialize the AnomalyDetectionPlot component.

        Args:
            ts_data: PySpark DataFrame with 'timestamp' and 'value' columns
            ad_data: PySpark DataFrame with 'timestamp' and 'value' columns
            sensor_id: Optional sensor identifier
            title: Optional custom title
            figsize: Figure size tuple
            linewidth: Line width for the time series
            anomaly_marker_size: Size of anomaly markers
            anomaly_color: Color for anomaly points
            ts_color: Color for time series line
            ax: Optional existing matplotlib axis to plot on
        """
        super().__init__()

        # Convert PySpark DataFrames to Pandas
        self.ts_data = ts_data.toPandas()
        self.ad_data = ad_data.toPandas() if ad_data is not None else None

        self.sensor_id = sensor_id
        self.title = title
        self.figsize = figsize
        self.linewidth = linewidth
        self.anomaly_marker_size = anomaly_marker_size
        self.anomaly_color = anomaly_color
        self.ts_color = ts_color
        self.ax = ax

        self._fig: Optional[Figure | SubFigure] = None
        self._validate_data()

    def _validate_data(self) -> None:
        """Validate that required columns exist in DataFrames."""
        required_cols = {"timestamp", "value"}

        if not required_cols.issubset(self.ts_data.columns):
            raise ValueError(
                f"ts_data must contain columns {required_cols}. "
                f"Got: {set(self.ts_data.columns)}"
            )

        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(self.ts_data["timestamp"]):
            self.ts_data["timestamp"] = pd.to_datetime(self.ts_data["timestamp"])

        # Ensure value is numeric
        if not pd.api.types.is_numeric_dtype(self.ts_data["value"]):
            self.ts_data["value"] = pd.to_numeric(
                self.ts_data["value"], errors="coerce"
            )

        if self.ad_data is not None and len(self.ad_data) > 0:
            if not required_cols.issubset(self.ad_data.columns):
                raise ValueError(
                    f"ad_data must contain columns {required_cols}. "
                    f"Got: {set(self.ad_data.columns)}"
                )

            # Convert ad_data timestamp
            if not pd.api.types.is_datetime64_any_dtype(self.ad_data["timestamp"]):
                self.ad_data["timestamp"] = pd.to_datetime(self.ad_data["timestamp"])

            # Convert ad_data value
            if not pd.api.types.is_numeric_dtype(self.ad_data["value"]):
                self.ad_data["value"] = pd.to_numeric(
                    self.ad_data["value"], errors="coerce"
                )

    def plot(self, ax: Optional[Axes] = None) -> Figure | SubFigure:
        """
        Generate the anomaly detection visualization.

        Args:
            ax: Optional matplotlib axis to plot on. If None, creates new figure.

        Returns:
            matplotlib.figure.Figure: The generated figure
        """
        # Use provided ax or instance ax
        use_ax = ax if ax is not None else self.ax

        if use_ax is None:
            self._fig, use_ax = plt.subplots(figsize=self.figsize)
        else:
            self._fig = use_ax.figure

        # Sort data by timestamp
        ts_sorted = self.ts_data.sort_values("timestamp")

        # Plot time series line
        use_ax.plot(
            ts_sorted["timestamp"],
            ts_sorted["value"],
            label="value",
            color=self.ts_color,
            linewidth=self.linewidth,
        )

        # Plot anomalies if available
        if self.ad_data is not None and len(self.ad_data) > 0:
            ad_sorted = self.ad_data.sort_values("timestamp")
            use_ax.scatter(
                ad_sorted["timestamp"],
                ad_sorted["value"],
                color=self.anomaly_color,
                s=self.anomaly_marker_size,
                label="anomaly",
                zorder=5,
            )

        # Set title
        if self.title:
            title = self.title
        elif self.sensor_id:
            n_anomalies = len(self.ad_data) if self.ad_data is not None else 0
            title = f"Sensor {self.sensor_id} - Anomalies: {n_anomalies}"
        else:
            n_anomalies = len(self.ad_data) if self.ad_data is not None else 0
            title = f"Anomaly Detection Results - Anomalies: {n_anomalies}"

        use_ax.set_title(title, fontsize=14)
        use_ax.set_xlabel("timestamp")
        use_ax.set_ylabel("value")
        use_ax.legend()
        use_ax.grid(True, alpha=0.3)

        if isinstance(self._fig, Figure):
            self._fig.tight_layout()

        return self._fig

    def save(
        self,
        filepath: Union[str, Path],
        dpi: int = 150,
        **kwargs,
    ) -> Path:
        """
        Save the visualization to file.

        Args:
            filepath: Output file path
            dpi: Dots per inch. Defaults to 150
            **kwargs: Additional arguments passed to savefig

        Returns:
            Path: The path to the saved file
        """

        assert self._fig is not None, "Plot the figure before saving."

        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if isinstance(self._fig, Figure):
            self._fig.savefig(filepath, dpi=dpi, **kwargs)

        return filepath
