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
    with pytest.raises(ValueError, match="Length of windows"):
        MSTLDecomposition(
            df=sample_time_series,
            value_column="value",
            timestamp_column="timestamp",
            periods=[7, 14],
            windows=[9],  # Wrong length
        )


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

    with pytest.raises(ValueError, match="Time series length"):
        MSTLDecomposition(
            df=df, value_column="value", timestamp_column="timestamp", periods=7
        )


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
