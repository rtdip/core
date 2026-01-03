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

import pytest
import pandas as pd
import numpy as np

from src.sdk.python.rtdip_sdk.pipelines.decomposition.pandas.mstl_decomposition import (
    MSTLDecomposition,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    SystemType,
    Libraries,
)


@pytest.fixture
def sample_time_series():
    """Create a sample time series with trend, seasonality, and noise."""
    np.random.seed(42)
    n_points = 365
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")
    trend = np.linspace(10, 20, n_points)
    seasonal = 5 * np.sin(2 * np.pi * np.arange(n_points) / 7)
    noise = np.random.randn(n_points) * 0.5
    value = trend + seasonal + noise

    return pd.DataFrame({"timestamp": dates, "value": value})


@pytest.fixture
def multi_seasonal_time_series():
    """Create a time series with multiple seasonal patterns."""
    np.random.seed(42)
    n_points = 24 * 60  # 60 days of hourly data
    dates = pd.date_range("2024-01-01", periods=n_points, freq="H")
    trend = np.linspace(10, 15, n_points)
    daily_seasonal = 5 * np.sin(2 * np.pi * np.arange(n_points) / 24)
    weekly_seasonal = 3 * np.sin(2 * np.pi * np.arange(n_points) / 168)
    noise = np.random.randn(n_points) * 0.5
    value = trend + daily_seasonal + weekly_seasonal + noise

    return pd.DataFrame({"timestamp": dates, "value": value})


def test_single_period(sample_time_series):
    """Test MSTL with single period."""
    decomposer = MSTLDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        periods=7,
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_7" in result.columns
    assert "residual" in result.columns


def test_multiple_periods(multi_seasonal_time_series):
    """Test MSTL with multiple periods."""
    decomposer = MSTLDecomposition(
        df=multi_seasonal_time_series,
        value_column="value",
        timestamp_column="timestamp",
        periods=[24, 168],  # Daily and weekly
        windows=[25, 169],
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_24" in result.columns
    assert "seasonal_168" in result.columns
    assert "residual" in result.columns


def test_list_period_input(sample_time_series):
    """Test MSTL with list of periods."""
    decomposer = MSTLDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        periods=[7, 14],
    )
    result = decomposer.decompose()

    assert "seasonal_7" in result.columns
    assert "seasonal_14" in result.columns


def test_invalid_windows_length(sample_time_series):
    """Test error handling for mismatched windows length."""
    decomposer = MSTLDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        periods=[7, 14],
        windows=[9],  # Wrong length
    )

    with pytest.raises(ValueError, match="Length of windows"):
        decomposer.decompose()


def test_invalid_column(sample_time_series):
    """Test error handling for invalid column."""
    with pytest.raises(ValueError, match="Column 'invalid' not found"):
        MSTLDecomposition(
            df=sample_time_series,
            value_column="invalid",
            timestamp_column="timestamp",
            periods=7,
        )


def test_nan_values(sample_time_series):
    """Test error handling for NaN values."""
    df = sample_time_series.copy()
    df.loc[50, "value"] = np.nan

    decomposer = MSTLDecomposition(
        df=df, value_column="value", timestamp_column="timestamp", periods=7
    )

    with pytest.raises(ValueError, match="contains NaN values"):
        decomposer.decompose()


def test_insufficient_data():
    """Test error handling for insufficient data."""
    df = pd.DataFrame(
        {
            "timestamp": pd.date_range("2024-01-01", periods=10, freq="D"),
            "value": np.random.randn(10),
        }
    )

    decomposer = MSTLDecomposition(
        df=df, value_column="value", timestamp_column="timestamp", periods=7
    )

    with pytest.raises(ValueError, match="Time series length"):
        decomposer.decompose()


def test_preserves_original(sample_time_series):
    """Test that decomposition doesn't modify original DataFrame."""
    original_df = sample_time_series.copy()

    decomposer = MSTLDecomposition(
        df=sample_time_series,
        value_column="value",
        timestamp_column="timestamp",
        periods=7,
    )
    decomposer.decompose()

    assert "trend" not in sample_time_series.columns
    pd.testing.assert_frame_equal(sample_time_series, original_df)


def test_system_type():
    """Test that system_type returns SystemType.PYTHON"""
    assert MSTLDecomposition.system_type() == SystemType.PYTHON


def test_libraries():
    """Test that libraries returns a Libraries instance"""
    libraries = MSTLDecomposition.libraries()
    assert isinstance(libraries, Libraries)


def test_settings():
    """Test that settings returns an empty dict"""
    settings = MSTLDecomposition.settings()
    assert isinstance(settings, dict)
    assert settings == {}


# =========================================================================
# Grouped Decomposition Tests
# =========================================================================


def test_grouped_single_column():
    """Test MSTL decomposition with single group column."""
    np.random.seed(42)
    n_hours = 24 * 30  # 30 days
    dates = pd.date_range("2024-01-01", periods=n_hours, freq="h")

    data = []
    for sensor in ["A", "B"]:
        daily = 5 * np.sin(2 * np.pi * np.arange(n_hours) / 24)
        weekly = 3 * np.sin(2 * np.pi * np.arange(n_hours) / 168)
        trend = np.linspace(10, 15, n_hours)
        noise = np.random.randn(n_hours) * 0.5
        values = trend + daily + weekly + noise

        for i in range(n_hours):
            data.append({"timestamp": dates[i], "sensor": sensor, "value": values[i]})

    df = pd.DataFrame(data)

    decomposer = MSTLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        periods=[24, 168],
        windows=[25, 169],
    )

    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_24" in result.columns
    assert "seasonal_168" in result.columns
    assert set(result["sensor"].unique()) == {"A", "B"}


def test_grouped_single_period():
    """Test MSTL with single period and groups."""
    np.random.seed(42)
    n_points = 100
    dates = pd.date_range("2024-01-01", periods=n_points, freq="D")

    data = []
    for sensor in ["A", "B"]:
        trend = np.linspace(10, 20, n_points)
        seasonal = 5 * np.sin(2 * np.pi * np.arange(n_points) / 7)
        noise = np.random.randn(n_points) * 0.5
        values = trend + seasonal + noise

        for i in range(n_points):
            data.append({"timestamp": dates[i], "sensor": sensor, "value": values[i]})

    df = pd.DataFrame(data)

    decomposer = MSTLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        periods=7,
    )

    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_7" in result.columns
    assert "residual" in result.columns


# =========================================================================
# Period String Tests
# =========================================================================


def test_period_string_hourly_from_5_second_data():
    """Test automatic period calculation with 'hourly' string."""
    np.random.seed(42)
    # 2 days of 5-second data
    n_samples = 2 * 24 * 60 * 12  # 2 days * 24 hours * 60 min * 12 samples/min
    dates = pd.date_range("2024-01-01", periods=n_samples, freq="5s")

    trend = np.linspace(10, 15, n_samples)
    # Hourly pattern
    hourly_pattern = 5 * np.sin(
        2 * np.pi * np.arange(n_samples) / 720
    )  # 720 samples per hour
    noise = np.random.randn(n_samples) * 0.5
    value = trend + hourly_pattern + noise

    df = pd.DataFrame({"timestamp": dates, "value": value})

    decomposer = MSTLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        periods="hourly",  # String period
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_720" in result.columns  # 3600 seconds / 5 seconds = 720
    assert "residual" in result.columns


def test_period_strings_multiple():
    """Test automatic period calculation with multiple period strings."""
    np.random.seed(42)
    n_samples = 3 * 24 * 12
    dates = pd.date_range("2024-01-01", periods=n_samples, freq="5min")

    trend = np.linspace(10, 15, n_samples)
    hourly = 5 * np.sin(2 * np.pi * np.arange(n_samples) / 12)
    daily = 3 * np.sin(2 * np.pi * np.arange(n_samples) / 288)
    noise = np.random.randn(n_samples) * 0.5
    value = trend + hourly + daily + noise

    df = pd.DataFrame({"timestamp": dates, "value": value})

    decomposer = MSTLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        periods=["hourly", "daily"],
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_12" in result.columns
    assert "seasonal_288" in result.columns
    assert "residual" in result.columns


def test_period_string_weekly_from_daily_data():
    """Test automatic period calculation with daily data."""
    np.random.seed(42)
    # 1 year of daily data
    n_days = 365
    dates = pd.date_range("2024-01-01", periods=n_days, freq="D")

    trend = np.linspace(10, 20, n_days)
    weekly = 5 * np.sin(2 * np.pi * np.arange(n_days) / 7)
    noise = np.random.randn(n_days) * 0.5
    value = trend + weekly + noise

    df = pd.DataFrame({"timestamp": dates, "value": value})

    decomposer = MSTLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        periods="weekly",
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_7" in result.columns
    assert "residual" in result.columns


def test_mixed_period_types():
    """Test mixing integer and string period specifications."""
    np.random.seed(42)
    n_samples = 3 * 24 * 12
    dates = pd.date_range("2024-01-01", periods=n_samples, freq="5min")

    trend = np.linspace(10, 15, n_samples)
    hourly = 5 * np.sin(2 * np.pi * np.arange(n_samples) / 12)
    custom = 3 * np.sin(2 * np.pi * np.arange(n_samples) / 50)
    noise = np.random.randn(n_samples) * 0.5
    value = trend + hourly + custom + noise

    df = pd.DataFrame({"timestamp": dates, "value": value})

    decomposer = MSTLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        periods=["hourly", 50],
    )
    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_12" in result.columns
    assert "seasonal_50" in result.columns
    assert "residual" in result.columns


def test_period_string_without_timestamp_raises_error():
    """Test that period strings require timestamp_column."""
    df = pd.DataFrame({"value": np.random.randn(100)})

    with pytest.raises(ValueError, match="timestamp_column must be provided"):
        decomposer = MSTLDecomposition(
            df=df,
            value_column="value",
            periods="hourly",  # String period without timestamp
        )
        decomposer.decompose()


def test_period_string_insufficient_data():
    """Test error handling when data insufficient for requested period."""
    # Only 10 samples at 1-second frequency
    dates = pd.date_range("2024-01-01", periods=10, freq="1s")
    df = pd.DataFrame({"timestamp": dates, "value": np.random.randn(10)})

    with pytest.raises(ValueError, match="not valid for this data"):
        decomposer = MSTLDecomposition(
            df=df,
            value_column="value",
            timestamp_column="timestamp",
            periods="hourly",  # Need 7200 samples for 2 cycles
        )
        decomposer.decompose()


def test_period_string_grouped():
    """Test period strings with grouped data."""
    np.random.seed(42)
    # 2 days of 5-second data per sensor
    n_samples = 2 * 24 * 60 * 12
    dates = pd.date_range("2024-01-01", periods=n_samples, freq="5s")

    data = []
    for sensor in ["A", "B"]:
        trend = np.linspace(10, 15, n_samples)
        hourly = 5 * np.sin(2 * np.pi * np.arange(n_samples) / 720)
        noise = np.random.randn(n_samples) * 0.5
        values = trend + hourly + noise

        for i in range(n_samples):
            data.append({"timestamp": dates[i], "sensor": sensor, "value": values[i]})

    df = pd.DataFrame(data)

    decomposer = MSTLDecomposition(
        df=df,
        value_column="value",
        timestamp_column="timestamp",
        group_columns=["sensor"],
        periods="hourly",
    )

    result = decomposer.decompose()

    assert "trend" in result.columns
    assert "seasonal_720" in result.columns
    assert set(result["sensor"].unique()) == {"A", "B"}
