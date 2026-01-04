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
Matplotlib-based visualization components for RTDIP.

This module provides static visualization classes using Matplotlib and Seaborn
for time series forecasting, anomaly detection, and model comparison.

Classes:
    ForecastPlot: Single sensor forecast with confidence intervals
    ForecastComparisonPlot: Forecast vs actual comparison
    MultiSensorForecastPlot: Grid view of multiple sensor forecasts
    ResidualPlot: Residuals over time analysis
    ErrorDistributionPlot: Histogram of forecast errors
    ScatterPlot: Actual vs predicted scatter plot
    ForecastDashboard: Comprehensive forecast dashboard

    ModelComparisonPlot: Compare model performance metrics
    ModelLeaderboardPlot: Ranked model performance
    ModelsOverlayPlot: Overlay multiple model forecasts
    ForecastDistributionPlot: Box plots of forecast distributions
    ComparisonDashboard: Model comparison dashboard

    AnomalyDetectionPlot: Static plot of time series with anomalies
"""

from .forecasting import (
    ForecastPlot,
    ForecastComparisonPlot,
    MultiSensorForecastPlot,
    ResidualPlot,
    ErrorDistributionPlot,
    ScatterPlot,
    ForecastDashboard,
)
from .comparison import (
    ModelComparisonPlot,
    ModelMetricsTable,
    ModelLeaderboardPlot,
    ModelsOverlayPlot,
    ForecastDistributionPlot,
    ComparisonDashboard,
)
from .anomaly_detection import (
    AnomalyDetectionPlot
)