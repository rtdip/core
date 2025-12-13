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
RTDIP Visualization Module.

This module provides standardized visualization components for time series forecasting,
anomaly detection, and model comparison. It supports both Matplotlib (static) and
Plotly (interactive) backends.

Submodules:
    - matplotlib: Static visualization using Matplotlib/Seaborn
    - plotly: Interactive visualization using Plotly

Example:
    ```python
    from rtdip_sdk.pipelines.visualization.matplotlib.forecasting import ForecastPlot
    from rtdip_sdk.pipelines.visualization.plotly.forecasting import ForecastPlotInteractive

    # Static plot
    plot = ForecastPlot(historical_df, forecast_df, forecast_start)
    fig = plot.plot()
    plot.save("forecast.png")

    # Interactive plot
    plot_interactive = ForecastPlotInteractive(historical_df, forecast_df, forecast_start)
    fig = plot_interactive.plot()
    plot_interactive.save("forecast.html")
    ```
"""

from . import config
from . import utils
from . import validation
from .interfaces import VisualizationBaseInterface
from .validation import VisualizationDataError
