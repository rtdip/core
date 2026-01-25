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
Utilities for calculating seasonal periods in time series decomposition.
"""

from typing import Union, List, Dict
import pandas as pd
from pandas import DataFrame as PandasDataFrame


# Mapping of period names to their duration in days
PERIOD_TIMEDELTAS = {
    "minutely": pd.Timedelta(minutes=1),
    "hourly": pd.Timedelta(hours=1),
    "daily": pd.Timedelta(days=1),
    "weekly": pd.Timedelta(weeks=1),
    "monthly": pd.Timedelta(days=30),  # Approximate month
    "quarterly": pd.Timedelta(days=91),  # Approximate quarter (3 months)
    "yearly": pd.Timedelta(days=365),  # Non-leap year
}


def calculate_period_from_frequency(
    df: PandasDataFrame,
    timestamp_column: str,
    period_name: str,
    min_cycles: int = 2,
) -> int:
    """
    Calculate the number of observations in a seasonal period based on sampling frequency.

    This function determines how many data points typically occur within a given time period
    (e.g., hourly, daily, weekly) based on the median sampling frequency of the time series.
    This is useful for time series decomposition methods like STL and MSTL that require
    period parameters expressed as number of observations.

    Parameters
    ----------
    df : PandasDataFrame
        Input DataFrame containing the time series data
    timestamp_column : str
        Name of the column containing timestamps
    period_name : str
        Name of the period to calculate. Supported values:
        'minutely', 'hourly', 'daily', 'weekly', 'monthly', 'quarterly', 'yearly'
    min_cycles : int, default=2
        Minimum number of complete cycles required in the data.
        The function returns None if the data doesn't contain enough observations
        for at least this many complete cycles.

    Returns
    -------
    int or None
        Number of observations per period, or None if:
        - The calculated period is less than 2
        - The data doesn't contain at least min_cycles complete periods

    Raises
    ------
    ValueError
        If period_name is not one of the supported values
        If timestamp_column is not in the DataFrame
        If the DataFrame has fewer than 2 rows

    Examples
    --------
    >>> # For 5-second sampling data, calculate hourly period
    >>> period = calculate_period_from_frequency(
    ...     df=sensor_data,
    ...     timestamp_column='EventTime',
    ...     period_name='hourly'
    ... )
    >>> # Returns: 720 (3600 seconds / 5 seconds per sample)

    >>> # For daily data, calculate weekly period
    >>> period = calculate_period_from_frequency(
    ...     df=daily_data,
    ...     timestamp_column='date',
    ...     period_name='weekly'
    ... )
    >>> # Returns: 7 (7 days per week)

    Notes
    -----
    - Uses median sampling frequency to be robust against irregular timestamps
    - For irregular time series, the period represents the typical number of observations
    - The actual period may vary slightly if sampling is irregular
    - Works with any time series where observations have associated timestamps
    """
    # Validate inputs
    if period_name not in PERIOD_TIMEDELTAS:
        valid_periods = ", ".join(PERIOD_TIMEDELTAS.keys())
        raise ValueError(
            f"Invalid period_name '{period_name}'. Must be one of: {valid_periods}"
        )

    if timestamp_column not in df.columns:
        raise ValueError(f"Column '{timestamp_column}' not found in DataFrame")

    if len(df) < 2:
        raise ValueError("DataFrame must have at least 2 rows to calculate periods")

    # Ensure timestamp column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df[timestamp_column]):
        raise ValueError(f"Column '{timestamp_column}' must be datetime type")

    # Sort by timestamp and calculate time differences
    df_sorted = df.sort_values(timestamp_column).reset_index(drop=True)
    time_diffs = df_sorted[timestamp_column].diff().dropna()

    if len(time_diffs) == 0:
        raise ValueError("Unable to calculate time differences from timestamps")

    # Calculate median sampling frequency
    median_freq = time_diffs.median()

    if median_freq <= pd.Timedelta(0):
        raise ValueError("Median time difference must be positive")

    # Calculate period as number of observations
    period_timedelta = PERIOD_TIMEDELTAS[period_name]
    period = int(period_timedelta / median_freq)

    # Validate period
    if period < 2:
        return None  # Period too small to be meaningful

    # Check if we have enough data for min_cycles
    data_length = len(df)
    if period * min_cycles > data_length:
        return None  # Not enough data for required cycles

    return period


def calculate_periods_from_frequency(
    df: PandasDataFrame,
    timestamp_column: str,
    period_names: Union[str, List[str]],
    min_cycles: int = 2,
) -> Dict[str, int]:
    """
    Calculate multiple seasonal periods from sampling frequency.

    Convenience function to calculate multiple periods at once.

    Parameters
    ----------
    df : PandasDataFrame
        Input DataFrame containing the time series data
    timestamp_column : str
        Name of the column containing timestamps
    period_names : str or List[str]
        Period name(s) to calculate. Can be a single string or list of strings.
        Supported values: 'minutely', 'hourly', 'daily', 'weekly', 'monthly',
        'quarterly', 'yearly'
    min_cycles : int, default=2
        Minimum number of complete cycles required in the data

    Returns
    -------
    Dict[str, int]
        Dictionary mapping period names to their calculated values (number of observations).
        Periods that are invalid or have insufficient data are excluded.

    Examples
    --------
    >>> # Calculate both hourly and daily periods
    >>> periods = calculate_periods_from_frequency(
    ...     df=sensor_data,
    ...     timestamp_column='EventTime',
    ...     period_names=['hourly', 'daily']
    ... )
    >>> # Returns: {'hourly': 720, 'daily': 17280}

    >>> # Use in MSTL decomposition
    >>> from rtdip_sdk.pipelines.decomposition.pandas import MSTLDecomposition
    >>> decomposer = MSTLDecomposition(
    ...     df=df,
    ...     value_column='Value',
    ...     timestamp_column='EventTime',
    ...     periods=['hourly', 'daily']  # Automatically calculated
    ... )
    """
    if isinstance(period_names, str):
        period_names = [period_names]

    periods = {}
    for period_name in period_names:
        period = calculate_period_from_frequency(
            df=df,
            timestamp_column=timestamp_column,
            period_name=period_name,
            min_cycles=min_cycles,
        )
        if period is not None:
            periods[period_name] = period

    return periods
