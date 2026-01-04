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

import pandas as pd
import plotly.graph_objects as go
from pyspark.sql import DataFrame as SparkDataFrame

from ..interfaces import PlotlyVisualizationInterface


class AnomalyDetectionPlotly(PlotlyVisualizationInterface):
    """
    Plot time series data with detected anomalies highlighted using Plotly.

    This component is functionally equivalent to the Matplotlib-based
    AnomalyDetectionPlot. It visualizes the full time series as a line and
    overlays detected anomalies as markers. Hover tooltips on anomaly markers
    explicitly show timestamp and value.
    """

    def __init__(
        self,
        ts_data: SparkDataFrame,
        ad_data: Optional[SparkDataFrame] = None,
        sensor_id: Optional[str] = None,
        title: Optional[str] = None,
        ts_color: str = "steelblue",
        anomaly_color: str = "red",
        anomaly_marker_size: int = 8,
    ) -> None:
        super().__init__()

        # Convert Spark DataFrames to Pandas
        self.ts_data = ts_data.toPandas()
        self.ad_data = ad_data.toPandas() if ad_data is not None else None

        self.sensor_id = sensor_id
        self.title = title
        self.ts_color = ts_color
        self.anomaly_color = anomaly_color
        self.anomaly_marker_size = anomaly_marker_size

        self._fig: Optional[go.Figure] = None
        self._validate_data()

    def _validate_data(self) -> None:
        """Validate required columns and enforce correct dtypes."""

        required_cols = {"timestamp", "value"}

        if not required_cols.issubset(self.ts_data.columns):
            raise ValueError(
                f"ts_data must contain columns {required_cols}. "
                f"Got: {set(self.ts_data.columns)}"
            )

        self.ts_data["timestamp"] = pd.to_datetime(self.ts_data["timestamp"])
        self.ts_data["value"] = pd.to_numeric(self.ts_data["value"], errors="coerce")

        if self.ad_data is not None and len(self.ad_data) > 0:
            if not required_cols.issubset(self.ad_data.columns):
                raise ValueError(
                    f"ad_data must contain columns {required_cols}. "
                    f"Got: {set(self.ad_data.columns)}"
                )

            self.ad_data["timestamp"] = pd.to_datetime(self.ad_data["timestamp"])
            self.ad_data["value"] = pd.to_numeric(
                self.ad_data["value"], errors="coerce"
            )

    def plot(self) -> go.Figure:
        """
        Generate the Plotly anomaly detection visualization.

        Returns:
            plotly.graph_objects.Figure
        """

        ts_sorted = self.ts_data.sort_values("timestamp")

        fig = go.Figure()

        # Time series line
        fig.add_trace(
            go.Scatter(
                x=ts_sorted["timestamp"],
                y=ts_sorted["value"],
                mode="lines",
                name="value",
                line=dict(color=self.ts_color),
            )
        )

        # Anomaly markers with explicit hover info
        if self.ad_data is not None and len(self.ad_data) > 0:
            ad_sorted = self.ad_data.sort_values("timestamp")
            fig.add_trace(
                go.Scatter(
                    x=ad_sorted["timestamp"],
                    y=ad_sorted["value"],
                    mode="markers",
                    name="anomaly",
                    marker=dict(
                        color=self.anomaly_color,
                        size=self.anomaly_marker_size,
                    ),
                    hovertemplate=(
                        "<b>Anomaly</b><br>"
                        "Timestamp: %{x}<br>"
                        "Value: %{y}<extra></extra>"
                    ),
                )
            )

        n_anomalies = len(self.ad_data) if self.ad_data is not None else 0

        if self.title:
            title = self.title
        elif self.sensor_id:
            title = f"Sensor {self.sensor_id} - Anomalies: {n_anomalies}"
        else:
            title = f"Anomaly Detection Results - Anomalies: {n_anomalies}"

        fig.update_layout(
            title=title,
            xaxis_title="timestamp",
            yaxis_title="value",
            template="plotly_white",
        )

        self._fig = fig
        return fig

    def save(
        self,
        filepath: Union[str, Path],
        **kwargs,
    ) -> Path:
        """
        Save the Plotly visualization to file.

        If the file suffix is `.html`, the figure is saved as an interactive HTML
        file. Otherwise, a static image is written (requires kaleido).

        Args:
            filepath: Output file path
            **kwargs: Additional arguments passed to write_html or write_image

        Returns:
            Path: The path to the saved file
        """
        assert self._fig is not None, "Plot the figure before saving."

        filepath = Path(filepath)
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if filepath.suffix.lower() == ".html":
            self._fig.write_html(filepath, **kwargs)
        else:
            self._fig.write_image(filepath, **kwargs)

        return filepath
