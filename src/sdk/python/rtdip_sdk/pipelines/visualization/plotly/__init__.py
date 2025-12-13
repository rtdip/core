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
Plotly-based interactive visualization components for RTDIP.

This module provides interactive visualization classes using Plotly
for time series forecasting, anomaly detection, and model comparison.

Classes:
    ForecastPlotInteractive: Interactive forecast with confidence intervals
    ForecastComparisonPlotInteractive: Interactive forecast vs actual comparison
    ResidualPlotInteractive: Interactive residuals over time
    ErrorDistributionPlotInteractive: Interactive error histogram
    ScatterPlotInteractive: Interactive actual vs predicted scatter

    ModelComparisonPlotInteractive: Interactive model performance comparison
    ModelsOverlayPlotInteractive: Interactive overlay of multiple models
    ForecastDistributionPlotInteractive: Interactive distribution comparison
"""

from .forecasting import (
    ForecastPlotInteractive,
    ForecastComparisonPlotInteractive,
    ResidualPlotInteractive,
    ErrorDistributionPlotInteractive,
    ScatterPlotInteractive,
)
from .comparison import (
    ModelComparisonPlotInteractive,
    ModelsOverlayPlotInteractive,
    ForecastDistributionPlotInteractive,
)
