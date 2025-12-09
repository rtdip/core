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
RTDIP Time Series Visualization Module

A standardized visualization library for time series forecasting, anomaly detection,
and model comparison tasks.

Usage:
    from amos_team_resources.visualization import forecasting
    from amos_team_resources.visualization import config
    from amos_team_resources.visualization import utils

    # Setup standard style
    utils.setup_plot_style()

    # Create visualizations
    forecasting.plot_forecast_with_confidence(...)
"""

__version__ = '1.0.0'

from . import config
from . import utils

# from . import forecasting
# from . import anomaly_detection
# from . import comparison

__all__ = [
    'config',
    'utils',
    # 'forecasting',
    # 'anomaly_detection',
    # 'comparison',
]
